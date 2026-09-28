// 复现用例：少数派旧 Leader 假确认客户端写入
// 路径：分区写入（旧 leader 返回成功）-> 愈合（日志被新 leader 截断）-> 状态机为空
// 修复后期望：少数派不得返回"提交成功"（挂起 pending 或明确拒绝），
// 且所有已确认（committed）的写入在愈合后必须真正一致。
require('../js/raft-config.js');
require('../js/network.js');
require('../js/raft.js');
const { Sim } = require('../js/sim.js');

let failures = 0;
function assert(cond, msg) {
  if (!cond) { failures += 1; console.error('  ✗ ' + msg); }
  else console.log('  ✓ ' + msg);
}
function runMs(sim, ms) {
  const steps = Math.ceil(ms / 20);
  for (let i = 0; i < steps; i++) sim.step(20);
}
function stateSummary(sim) {
  return sim.nodeOrder
    .map((id) => 'N' + id + '=' + JSON.stringify(sim.nodes[id].state))
    .join('  ');
}

// ---------- 场景 A：分区写入 -> 愈合 -> 状态机为空 ----------
console.log('场景A 少数派旧 Leader 不得假确认写入');
{
  const sim = new Sim({ nodeCount: 3, seed: 31, autoSnapshot: false });
  runMs(sim, 5000);
  const oldLeader = sim.leader();
  assert(!!oldLeader, '已选出 Leader N' + (oldLeader && oldLeader.id));

  // 把 Leader 单独隔离成少数派，立刻（CheckQuorum 宽限期内）写入
  sim.isolate(oldLeader.id);
  const result = sim.submitCommand({ type: 'set', key: 'p', value: 'lost' });
  console.log('  [客户端] 分区中提交 set p=lost，返回值：' + JSON.stringify(result));

  assert(!(result.ok && !result.pending && result.committed !== false),
    '少数派旧 Leader 不得返回"提交成功"（实际：' + JSON.stringify(result) + '）');
  assert(result.ok !== true || result.pending === true || result.committed === false,
    '若被受理必须明确为 pending（挂起等待多数派），而非已提交');

  // 宽限期后旧 Leader 降级，多数派选出新 Leader
  runMs(sim, 4000);
  const newLeader = sim.leader();
  assert(!!newLeader && newLeader.id !== oldLeader.id,
    '多数派选出新 Leader N' + (newLeader && newLeader.id));
  assert(oldLeader.role !== 'leader', '旧 Leader 已因 CheckQuorum 降级');

  // 愈合：旧 Leader 上未提交的日志被新 Leader 覆盖截断
  sim.heal();
  runMs(sim, 3000);

  const status = sim.commandStatus(result.uid);
  console.log('  [客户端] 愈合后该写入最终状态：' + JSON.stringify(status));
  assert(status && status.status !== 'pending' && status.status !== undefined,
    '挂起的写入在愈合后必须有明确终态（committed 或 lost），不得永远悬而未决');
  assert(status && status.status === 'lost',
    '该写入从未获多数派提交，必须明确告知客户端失败（lost），实际：' + JSON.stringify(status));
  assert(!!(status && status.reason), '失败必须附带原因说明：' + (status && status.reason));

  const emptyEverywhere = sim.nodeOrder.every((id) => sim.nodes[id].state.p === undefined);
  assert(emptyEverywhere, '状态机与客户端被告知的结果一致：p 未生效（状态机为空）');
  const statesEqual = sim.nodeOrder
    .map((id) => JSON.stringify(sim.nodes[id].state))
    .every((v, _, arr) => v === arr[0]);
  assert(statesEqual, '愈合后各节点状态机一致：' + stateSummary(sim));
}

// ---------- 场景 B：已确认的写入，愈合后必须真正一致 ----------
console.log('场景B 已确认（committed）写入愈合后真正一致');
{
  const sim = new Sim({ nodeCount: 3, seed: 31, autoSnapshot: false });
  runMs(sim, 5000);
  const leader = sim.leader();
  assert(!!leader, '已选出 Leader N' + (leader && leader.id));

  // 健康集群写入：必须能等到 committed 确认
  const r1 = sim.submitCommand({ type: 'set', key: 'ok', value: '1' });
  assert(r1.ok && r1.pending, '健康集群受理写入并进入 pending：' + JSON.stringify(r1));
  runMs(sim, 1500);
  const s1 = sim.commandStatus(r1.uid);
  assert(s1 && s1.status === 'committed',
    '多数派提交后客户端收到 committed 确认，实际：' + JSON.stringify(s1));

  // 隔离一个 follower（少数派），多数派继续写入并确认
  const follower = sim.nodeOrder.find((id) => id !== leader.id);
  sim.isolate(follower);
  const r2 = sim.submitCommand({ type: 'set', key: 'ok2', value: '2' });
  runMs(sim, 1500);
  const s2 = sim.commandStatus(r2.uid);
  assert(s2 && s2.status === 'committed',
    '多数派分区仍可提交并确认第二条写入，实际：' + JSON.stringify(s2));

  // 愈合后：两条已确认写入必须在所有节点状态机中真实存在
  sim.heal();
  runMs(sim, 3000);
  const allApplied = sim.nodeOrder.every(
    (id) => sim.nodes[id].state.ok === '1' && sim.nodes[id].state.ok2 === '2'
  );
  assert(allApplied, '愈合后所有节点状态机都包含已确认写入 ok=1, ok2=2：' + stateSummary(sim));
  const statesEqual = sim.nodeOrder
    .map((id) => JSON.stringify(sim.nodes[id].state))
    .every((v, _, arr) => v === arr[0]);
  assert(statesEqual, '愈合后所有节点状态机完全一致');
}

if (failures > 0) {
  console.error('\n复现用例失败：' + failures + ' 个断言未通过（少数派写入被假确认/悬而未决）');
  process.exit(1);
} else {
  console.log('\n分区写入用例全部通过 ✔');
}
