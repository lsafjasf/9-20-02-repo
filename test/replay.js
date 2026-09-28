// ============================================================
// replay.js - 确定性重放用例（零依赖，node test/replay.js）
//
// 验证：
//   1. 同一 seed 两次运行，选举/投票/心跳/提交的事件序列逐条完全一致；
//   2. 不同 seed 时间线不同（谁先选上、各事件发生时刻不同），
//      但收敛结论不变：最终同一状态机、同 term 无双 leader、无告警。
//   3. 默认随机源（无 seed）同样收敛，证明注入随机不改变收敛结论。
// ============================================================
'use strict';
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const crypto = require('crypto');

// raft.js 依赖 window，在 Node 中 shim 后加载
const sandbox = { window: {}, console: console };
vm.createContext(sandbox);
vm.runInContext(fs.readFileSync(path.join(__dirname, '..', 'js', 'raft.js'), 'utf8'), sandbox);
const RAFT = sandbox.window.RAFT;

// 审计重放需要比对完整事件序列，关闭事件面板的滚动截断（仅影响本测试）
RAFT.CFG.EVENT_LIMIT = 1000000;

// ---------------- 固定故障脚本（不依赖随机结果，保证两次注入完全相同） ----------------
function runScenario(seed) {
  RAFT.resetClock();
  RAFT.setSeed(seed === null ? null : seed);

  const c = new RAFT.Cluster(5);
  c.nodes[3].delay = 1; // 节点4：慢节点（单向 +200ms），让选举结果更依赖随机超时

  // t=1200 提交第一条命令（首个 leader 应已稳定）
  RAFT.schedule(1200, () => c.submit('SET x=1'));
  // t=2500 宕掉当前 leader，t=4500 恢复
  RAFT.schedule(2500, () => { const l = c.leader(); if (l) l.crash(); });
  RAFT.schedule(4500, () => { for (const n of c.nodes) if (n.down) n.recover(); });
  // t=6000 提交第二条
  RAFT.schedule(6000, () => c.submit('SET y=2'));
  // t=7500 把当前 leader 隔离进少数派分区 A，t=11000 愈合
  RAFT.schedule(7500, () => { const l = c.leader(); if (l) l.setPartition('A'); });
  RAFT.schedule(11000, () => { for (const n of c.nodes) n.setPartition(null); });
  // t=12000 分区愈合、新 leader 稳定后提交第三条
  RAFT.schedule(12000, () => c.submit('SET z=3'));

  // 提交进度采样：固定墙钟点记录各节点 commitIndex/lastApplied
  const commitTrace = [];
  const total = 14000;
  const step = 200;
  for (let t = step; t <= total; t += step) {
    RAFT.tick(step);
    commitTrace.push({
      t: RAFT.currentTime(),
      ci: c.nodes.map(n => n.commitIndex),
      la: c.nodes.map(n => n.lastApplied)
    });
  }

  return { cluster: c, commitTrace: commitTrace };
}

// ---------------- 指纹 / 断言 ----------------
function fingerprint(result) {
  const c = result.cluster;
  return JSON.stringify({
    events: c.events.map(e => ({ time: e.time, type: e.type, text: e.text, bad: e.bad })),
    commitTrace: result.commitTrace,
    nodes: c.nodes.map(n => ({
      id: n.id, term: n.currentTerm, state: n.state,
      commitIndex: n.commitIndex, lastApplied: n.lastApplied,
      log: n.log.map(x => ({ term: x.term, cmd: x.cmd })),
      sm: n.sm, down: n.down, partition: n.partition
    })),
    epochs: c.epochs
  });
}

function sha(s) { return crypto.createHash('sha256').update(s).digest('hex').slice(0, 16); }

function convergenceReport(result) {
  const c = result.cluster;
  const alive = c.nodes.filter(n => !n.down);
  const sms = alive.map(n => JSON.stringify(n.sm));
  const las = alive.map(n => n.lastApplied);
  const smConsistent = sms.every(x => x === sms[0]);
  const laConsistent = las.every(x => x === las[0]);
  const violations = [];
  Object.keys(c.epochs).forEach(term => {
    const ids = c.epochs[term].map(e => e.id);
    if (ids.some((id, i) => ids.indexOf(id) !== i)) violations.push(term);
  });
  const badEvents = c.events.filter(e => e.bad).map(e => '[' + e.time + 'ms] ' + e.text);
  const lead = c.leader();
  return {
    leader: lead ? '节点' + (lead.id + 1) + '(term=' + lead.currentTerm + ')' : '无',
    sm: sms[0],
    smConsistent: smConsistent,
    lastApplied: las.join(','),
    laConsistent: laConsistent,
    dualLeaderTerms: violations,
    badEvents: badEvents,
    pending: c.clientPending,
    eventCount: c.events.length
  };
}

function assert(cond, msg) { if (!cond) throw new Error('断言失败：' + msg); }

function firstDivergence(a, b) {
  const ea = a.cluster.events, eb = b.cluster.events;
  const n = Math.min(ea.length, eb.length);
  for (let i = 0; i < n; i++) {
    const x = ea[i], y = eb[i];
    if (x.time !== y.time || x.type !== y.type || x.text !== y.text) {
      return { index: i, a: x, b: y };
    }
  }
  if (ea.length !== eb.length) return { index: n, a: ea[n] || null, b: eb[n] || null };
  return null;
}

function fmtEvent(e) {
  return e ? ('[' + String(e.time).padStart(6) + 'ms] (' + e.type + ') ' + e.text) : '<事件结束>';
}

// 提取关键里程碑：选举发起 / 成为 leader / 投票 / 新 leader 首轮心跳 / 提交推进 / 客户端命令
function milestones(result, until) {
  const out = [];
  let seq = 0;
  const heartbeatLogged = {}; // term -> 是否已记录该 term leader 的首轮心跳
  for (const e of result.cluster.events) {
    if (until !== undefined && e.time > until) break;
    const mySeq = seq++;
    if (e.type === 'elec' || e.type === 'cmd' || e.bad) {
      out.push({ t: e.time, seq: mySeq, text: e.text });
    } else if (e.type === 'vote') {
      out.push({ t: e.time, seq: mySeq, text: '[投票] ' + e.text });
    } else if (e.type === 'app') {
      const m = e.text.match(/term=(\d+)/);
      const term = m ? m[1] : '?';
      if (!heartbeatLogged[term]) {
        heartbeatLogged[term] = true;
        out.push({ t: e.time, seq: mySeq, text: '[心跳] term=' + term + ' leader 首轮 AppendEntries：' + e.text });
      }
    }
  }
  // 提交推进来自固定采样点（取多数派首次达到某 commitIndex 的时刻）
  let lastMajor = 0;
  for (const s of result.commitTrace) {
    if (until !== undefined && s.t > until) break;
    const sorted = s.ci.slice().sort((x, y) => x - y);
    const major = sorted[Math.floor(sorted.length / 2)];
    if (major > lastMajor) {
      out.push({ t: s.t, seq: seq++,
        text: '[提交] 多数派 commitIndex -> ' + major + '（各节点 ' + s.ci.join(',') + '）' });
      lastMajor = major;
    }
  }
  return out.sort((x, y) => x.t - y.t || x.seq - y.seq)
    .map((o, i) => String(i + 1).padStart(3) + ' [' + String(o.t).padStart(6) + 'ms] ' + o.text);
}

// ---------------- 运行 ----------------
const SEED_A = 20260928;
const SEED_B = 424242;

console.log('场景：5 节点（节点4 单向 +200ms 慢节点），固定脚本：');
console.log('  t=1200 SET x=1 → t=2500 宕 leader / t=4500 恢复 → t=6000 SET y=2');
console.log('  → t=7500 leader 隔离进少数派 / t=11000 愈合 → t=12000 SET z=3，跑到 14000ms\n');

console.log('=== 同 seed=' + SEED_A + ' 运行两次 ===');
const a1 = runScenario(SEED_A);
const a2 = runScenario(SEED_A);
const fa1 = fingerprint(a1), fa2 = fingerprint(a2);
console.log('第 1 次：事件 ' + a1.cluster.events.length + ' 条，指纹 ' + sha(fa1));
console.log('第 2 次：事件 ' + a2.cluster.events.length + ' 条，指纹 ' + sha(fa2));
assert(fa1 === fa2, '同 seed 两次运行的完整时间线（事件序列+提交轨迹+节点状态）必须逐条一致');
console.log('>>> 两次运行逐条完全一致 ✓\n');

const mA1 = milestones(a1, 5200), mA2 = milestones(a2, 5200);
assert(JSON.stringify(mA1) === JSON.stringify(mA2), '同 seed 关键里程碑必须一致');
console.log('--- seed=' + SEED_A + ' 前 5.2s 的选举/投票/心跳/提交里程碑（两次运行逐字相同）---');
console.log(mA1.join('\n'));
console.log('');

console.log('=== 不同 seed 对照（seed=' + SEED_B + '） ===');
const b1 = runScenario(SEED_B);
const fb1 = fingerprint(b1);
console.log('seed=' + SEED_A + '：事件 ' + a1.cluster.events.length + ' 条，指纹 ' + sha(fa1));
console.log('seed=' + SEED_B + '：事件 ' + b1.cluster.events.length + ' 条，指纹 ' + sha(fb1));
assert(fa1 !== fb1, '不同 seed 应产生不同时间线');
const d = firstDivergence(a1, b1);
assert(d, '不同 seed 的事件序列应当存在分歧点');
console.log('>>> 不同 seed 时间线不同，首个分歧点在事件 #' + d.index + '：');
console.log('    seed=' + SEED_A + '：' + fmtEvent(d.a));
console.log('    seed=' + SEED_B + '：' + fmtEvent(d.b));
const mB1 = milestones(b1, 5200);
assert(JSON.stringify(mA1) !== JSON.stringify(mB1), '不同 seed 的关键里程碑应当不同');
console.log('');
console.log('--- seed=' + SEED_B + ' 同窗口里程碑（对比可见选举/心跳时刻与胜出节点不同）---');
console.log(mB1.join('\n'));
console.log('');

// 事件序列里必须真实覆盖 选举/投票/心跳(AppendEntries)/提交
for (const [r, name] of [[a1, 'seed=' + SEED_A], [b1, 'seed=' + SEED_B]]) {
  const types = new Set(r.cluster.events.map(e => e.type));
  assert(types.has('elec'), name + ' 缺少选举事件');
  assert(types.has('vote'), name + ' 缺少投票 RPC');
  assert(types.has('app'), name + ' 缺少 AppendEntries/心跳 RPC');
  const committed = r.commitTrace[r.commitTrace.length - 1].la.filter(x => x > 0).length;
  assert(committed >= 3, name + ' 提交未推进');
}

console.log('=== 收敛结论对照（注入随机不改变收敛结论） ===');
for (const [name, r] of [['seed=' + SEED_A + ' 第1次', a1], ['seed=' + SEED_A + ' 第2次', a2], ['seed=' + SEED_B, b1]]) {
  const rep = convergenceReport(r);
  console.log('--- ' + name + ' ---');
  console.log('  最终 leader      : ' + rep.leader);
  console.log('  存活节点状态机   : ' + rep.sm + '（一致 ' + (rep.smConsistent ? '✓' : '✗') + '）');
  console.log('  各节点 lastApplied: ' + rep.lastApplied + '（一致 ' + (rep.laConsistent ? '✓' : '✗') + '）');
  console.log('  同 term 双 leader : ' + (rep.dualLeaderTerms.length ? 'term ' + rep.dualLeaderTerms + ' ✗' : '无 ✓'));
  console.log('  安全告警事件      : ' + (rep.badEvents.length ? rep.badEvents.join('; ') + ' ✗' : '无 ✓'));
  console.log('  未完成客户端请求  : ' + rep.pending);
  assert(rep.smConsistent && rep.laConsistent, name + ' 存活节点未收敛到一致状态');
  assert(rep.dualLeaderTerms.length === 0, name + ' 出现同 term 双 leader');
  assert(rep.badEvents.length === 0, name + ' 出现安全告警');
  assert(rep.pending === 0, name + ' 存在未完成客户端请求');
  assert(rep.sm === JSON.stringify({ x: '1', y: '2', z: '3' }), name + ' 最终状态机内容错误: ' + rep.sm);
}

console.log('\n=== 默认随机源（setSeed(null)，Math.random） ===');
const r1 = runScenario(null);
const r2 = runScenario(null);
const rep1 = convergenceReport(r1), rep2 = convergenceReport(r2);
console.log('随机运行 1：leader=' + rep1.leader + '，状态机 ' + rep1.sm + '，事件 ' + rep1.eventCount + ' 条');
console.log('随机运行 2：leader=' + rep2.leader + '，状态机 ' + rep2.sm + '，事件 ' + rep2.eventCount + ' 条');
assert(rep1.sm === JSON.stringify({ x: '1', y: '2', z: '3' }) &&
       rep2.sm === JSON.stringify({ x: '1', y: '2', z: '3' }), '默认随机源未收敛到预期状态机');
assert(rep1.dualLeaderTerms.length === 0 && rep2.dualLeaderTerms.length === 0, '默认随机源出现双 leader');
assert(rep1.badEvents.length === 0 && rep2.badEvents.length === 0, '默认随机源出现安全告警');
assert(rep1.pending === 0 && rep2.pending === 0, '默认随机源存在未完成请求');
console.log('>>> 默认随机同样收敛，且时间线天然不同（指纹 ' + sha(fingerprint(r1)) + ' vs ' + sha(fingerprint(r2)) + '）✓\n');

// 恢复默认随机源，避免影响浏览器 UI 的后续行为
RAFT.setSeed(null);
console.log('全部重放断言通过 ✓');
