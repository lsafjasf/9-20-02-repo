// ============================================================
// replay.js - 可注入随机源的重放一致性用例（Node 直接运行）
//
//   node test/replay.js
//
// 验证：
//   1) 同一 seed 运行两次：选举/投票/心跳/提交的事件序列逐条一致；
//   2) 不同 seed：事件序列不同；
//   3) 注入随机不改变收敛结论：两种 seed 下集群最终都收敛
//      （唯一 leader、各存活节点状态机与日志一致）。
// ============================================================
'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// raft.js 是浏览器 IIFE，挂到 window 上；在 Node 里垫一个 window
global.window = {};
const src = fs.readFileSync(path.join(__dirname, '..', 'js', 'raft.js'), 'utf8');
eval(src);
const RAFT = global.window.RAFT;

// 固定剧本：只依赖模拟时钟，不依赖任何真实随机
function runScenario(seed) {
  RAFT.setSeed(seed);
  RAFT.resetClock();
  const cluster = new RAFT.Cluster(5);

  // 在固定模拟时刻注入客户端请求与故障
  RAFT.schedule(1500, () => cluster.submit('SET x=1'));
  RAFT.schedule(1600, () => cluster.submit('SET y=hello'));
  RAFT.schedule(2500, () => cluster.node(3).crash());
  RAFT.schedule(2600, () => cluster.submit('SET z=42'));
  RAFT.schedule(4500, () => cluster.node(3).recover());
  RAFT.schedule(5000, () => cluster.submit('DEL x'));

  // 推进到 8000ms：最后 3000ms 无新请求，集群应完全收敛
  for (let i = 0; i < 80; i++) RAFT.tick(100);

  const alive = cluster.nodes.filter(n => !n.down);
  const leaders = alive.filter(n => n.state === 'leader');
  const smSig = alive.map(n => JSON.stringify(n.sm));
  const logSig = alive.map(n => JSON.stringify(n.log));
  const applied = alive.map(n => n.lastApplied);

  return {
    events: cluster.events.map(e => [e.time, e.type, e.text]),
    converged:
      leaders.length === 1 &&
      smSig.every(s => s === smSig[0]) &&
      logSig.every(s => s === logSig[0]) &&
      applied.every(a => a === applied[0]) &&
      applied[0] === cluster.nodes[leaders[0].id].commitIndex,
    leader: leaders.length ? leaders[0].id + 1 : null,
    term: leaders.length ? leaders[0].currentTerm : null,
    lastApplied: applied[0],
    sm: alive[0].sm
  };
}

function digest(events) {
  return crypto.createHash('sha1').update(JSON.stringify(events)).digest('hex').slice(0, 12);
}

function diffAt(a, b) {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    if (JSON.stringify(a[i]) !== JSON.stringify(b[i])) return i;
  }
  return a.length === b.length ? -1 : n;
}

let failed = false;
function check(name, ok, detail) {
  console.log((ok ? '  ✓ ' : '  ✗ ') + name + (detail ? '  ' + detail : ''));
  if (!ok) failed = true;
}

console.log('— 同一 seed（audit-42）运行两次 —');
const r1 = runScenario('audit-42');
const r2 = runScenario('audit-42');
console.log('  run#1 事件数=' + r1.events.length + '  指纹=' + digest(r1.events));
console.log('  run#2 事件数=' + r2.events.length + '  指纹=' + digest(r2.events));
check('事件序列逐条一致', JSON.stringify(r1.events) === JSON.stringify(r2.events),
  '(diff 起始索引=' + diffAt(r1.events, r2.events) + ')');
check('run#1 收敛', r1.converged,
  'leader=节点' + r1.leader + ' term=' + r1.term + ' lastApplied=' + r1.lastApplied);
check('run#2 收敛', r2.converged,
  'leader=节点' + r2.leader + ' term=' + r2.term + ' lastApplied=' + r2.lastApplied);

console.log('— 不同 seed（audit-43）—');
const r3 = runScenario('audit-43');
console.log('  run#3 事件数=' + r3.events.length + '  指纹=' + digest(r3.events));
check('事件序列与 seed=audit-42 不同', JSON.stringify(r1.events) !== JSON.stringify(r3.events),
  '(首个差异事件索引=' + diffAt(r1.events, r3.events) + ')');
check('run#3 同样收敛（注入随机不改变收敛结论）', r3.converged,
  'leader=节点' + r3.leader + ' term=' + r3.term + ' lastApplied=' + r3.lastApplied);

console.log('— 默认不注入 seed 时仍然随机 —');
const r4 = runScenario(undefined);
const r5 = runScenario(undefined);
check('两次默认运行事件序列不同', JSON.stringify(r4.events) !== JSON.stringify(r5.events),
  '(指纹 ' + digest(r4.events) + ' vs ' + digest(r5.events) + ')');

console.log('— 抽样：seed=audit-42 的事件时间线（前 6 条 + 选举/上任关键事件）—');
const key = r1.events.filter(e => /成为 leader|发起选举/.test(e[2]));
r1.events.slice(0, 6).concat(key.slice(0, 6)).forEach(e =>
  console.log('  [' + String(e[0]).padStart(5) + 'ms] ' + e[1] + '  ' + e[2]));

console.log(failed ? '\nFAIL' : '\nPASS');
process.exit(failed ? 1 : 0);
