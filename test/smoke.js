// Node 冒烟测试：验证选举 / 复制 / 提交 / 分区 / 快照恢复
require('../js/raft-config.js');
require('../js/network.js');
require('../js/raft.js');
require('../js/sim.js');

const { Sim } = require('../js/sim.js');

let failures = 0;
function assert(cond, msg) {
  if (!cond) {
    failures += 1;
    console.error('  ✗ ' + msg);
  } else {
    console.log('  ✓ ' + msg);
  }
}

function runMs(sim, ms) {
  const steps = Math.ceil(ms / 20);
  for (let i = 0; i < steps; i++) sim.step(20);
}

function leaderOf(sim) {
  return sim.leader();
}

function logsSummary(sim) {
  return sim.nodeOrder.map((id) => {
    const n = sim.nodes[id];
    return (
      'N' + id +
      '[' + n.role + ' t' + n.currentTerm +
      ' ci' + n.commitIndex +
      ' la' + n.lastApplied +
      ' snap' + (n.snapshot ? n.snapshot.lastIncludedIndex : '-') +
      '] ' +
      n.log.map((e) => '#' + e.index + ':t' + e.term).join(',')
    );
  });
}

// ---------- 场景 1：选举 ----------
console.log('场景1 初始选举');
{
  const sim = new Sim({ nodeCount: 3, seed: 7, autoSnapshot: false });
  runMs(sim, 5000);
  const leader = leaderOf(sim);
  assert(!!leader, '5000ms 内产生唯一 Leader：' + (leader ? 'N' + leader.id : '无'));
  const leaders = sim.nodeOrder.filter((id) => sim.nodes[id].role === 'leader');
  assert(leaders.length === 1, '同时只有一个 Leader，实际 ' + leaders.length);
  const term = leader.currentTerm;
  assert(term >= 1, 'Leader term >= 1，实际 ' + term);
  const followers = sim.nodeOrder.filter((id) => sim.nodes[id].role === 'follower');
  assert(followers.length === 2, '其余 2 节点为 Follower');
}

// ---------- 场景 2：复制提交 ----------
console.log('场景2 日志复制与提交');
{
  const sim = new Sim({ nodeCount: 3, seed: 11, autoSnapshot: false });
  runMs(sim, 5000);
  const leader = leaderOf(sim);
  assert(!!leader, '已选出 Leader N' + (leader && leader.id));
  const r1 = sim.submitCommand({ type: 'set', key: 'x', value: '1' });
  assert(r1.ok, 'Leader 接受 set x=1');
  sim.submitCommand({ type: 'set', key: 'y', value: '2' });
  runMs(sim, 1500);
  assert(sim.nodes[leader.id].state.x === '1', 'Leader 状态机 x=1');
  const applied = sim.nodeOrder
    .filter((id) => id !== leader.id)
    .every((id) => sim.nodes[id].state.x === '1' && sim.nodes[id].state.y === '2');
  assert(applied, '两个 Follower 状态机都应用 x=1,y=2');
  const logsEqual = sim.nodeOrder
    .map((id) => JSON.stringify(sim.nodes[id].log.map((e) => e.index + ':' + e.term)))
    .every((v, _, arr) => v === arr[0]);
  assert(logsEqual, '三节点日志（index/term）完全一致');
  assert(sim.nodeOrder.every((id) => sim.nodes[id].commitIndex === sim.nodes[leader.id].lastLogIndex),
    '所有节点 commitIndex 一致并到达日志末尾');
}

// ---------- 场景 3：分区无双 Leader + 恢复一致 ----------
console.log('场景3 少数派分区不出现双 Leader，恢复后一致');
{
  const sim = new Sim({ nodeCount: 5, seed: 23, autoSnapshot: false });
  runMs(sim, 5000);
  const leader = leaderOf(sim);
  assert(!!leader, '已选出 Leader N' + (leader && leader.id));
  let majorityLeader = null;
  let isolated = null;
  if (leader.id === 5) {
    // leader 在多数派：隔离另一个节点
    isolated = 4;
    majorityLeader = leader;
  } else {
    isolated = 5;
    majorityLeader = leader;
  }
  sim.isolate(isolated);
  sim.submitCommand({ type: 'set', key: 'a', value: 'maj' });
  runMs(sim, 4000);
  const leadersDuringPartition = sim.nodeOrder.filter(
    (id) => sim.nodes[id].role === 'leader' && !sim.nodes[id].down
  );
  assert(leadersDuringPartition.length === 1,
    '分区期间全集群仅 1 个 Leader：' + leadersDuringPartition.join(','));
  assert(leadersDuringPartition[0] !== isolated,
    'Leader 位于多数派，被隔离节点 N' + isolated + ' 不是 Leader');
  assert(sim.nodes[isolated].state.a !== 'maj', '隔离节点未提交多数派新日志');
  // 隔离节点本地可能有旧 leader 身份，必须已被 CheckQuorum 降级或无法连任
  assert(sim.nodes[isolated].role !== 'leader', '被隔离节点不是 Leader（CheckQuorum 生效）');
  sim.heal();
  runMs(sim, 3000);
  const sameLogs = sim.nodeOrder
    .map((id) => JSON.stringify(sim.nodes[id].log.map((e) => e.index + ':' + e.term)))
    .every((v, _, arr) => v === arr[0]);
  assert(sameLogs, '恢复后所有节点日志一致');
  assert(sim.nodeOrder.every((id) => sim.nodes[id].state.a === 'maj'),
    '恢复后所有节点状态机一致（a=maj）');
  assert(!!sim.leader(), '恢复后集群仍有 Leader');
}

// ---------- 场景 4：Leader 在少数派 -> 降级 + 多数派新 Leader 覆盖 ----------
console.log('场景4 Leader 被隔离后主动降级，恢复后强制覆盖一致');
{
  const sim = new Sim({ nodeCount: 3, seed: 31, autoSnapshot: false });
  runMs(sim, 5000);
  const leader = leaderOf(sim);
  assert(!!leader, '已选出 Leader N' + (leader && leader.id));
  sim.isolate(leader.id);
  runMs(sim, 3500);
  assert(sim.nodes[leader.id].role !== 'leader', '旧 Leader 在隔离区主动降级');
  const newLeader = sim.leader();
  assert(!!newLeader, '多数派选出新 Leader N' + (newLeader && newLeader.id));
  assert(newLeader.id !== leader.id, '新 Leader 不是旧 Leader');
  sim.submitCommand({ type: 'set', key: 'b', value: 'new' });
  runMs(sim, 1200);
  // 旧 leader 在隔离期间无法提交
  sim.heal();
  runMs(sim, 3000);
  const sameLogs = sim.nodeOrder
    .map((id) => JSON.stringify(sim.nodes[id].log.map((e) => e.index + ':' + e.term)))
    .every((v, _, arr) => v === arr[0]);
  assert(sameLogs, '恢复后旧 Leader 日志被强制覆盖，日志一致');
  assert(sim.nodes[leader.id].state.b === 'new', '旧 Leader 状态机追平 b=new');
  const leaders = sim.nodeOrder.filter((id) => sim.nodes[id].role === 'leader');
  assert(leaders.length === 1, '全程无双 Leader');
}

// ---------- 场景 5：快照压缩 + InstallSnapshot 追平 ----------
console.log('场景5 Snapshot / log compaction');
{
  const sim = new Sim({ nodeCount: 5, seed: 41, autoSnapshot: true });
  runMs(sim, 5000);
  const leader = leaderOf(sim);
  // 让一个 follower 长期宕机
  const laggard = sim.nodeOrder.find((id) => id !== leader.id);
  sim.setDown(laggard, true);
  for (let i = 0; i < 8; i++) {
    sim.submitCommand({ type: 'set', key: 'k' + i, value: 'v' + i });
    runMs(sim, 400);
  }
  runMs(sim, 300);
  const r = sim.compactNode(leader.id);
  assert(r.ok || leader.snapshot, 'Leader 手动压缩到 lastApplied：' + JSON.stringify(r));
  assert(leader.snapshot && leader.snapshot.lastIncludedIndex >= 1, 'Leader 已有快照');
  assert(leader.log.every((e) => e.index > leader.snapshot.lastIncludedIndex),
    'Leader 旧日志已被截断');
  assert(Object.keys(leader.snapshot.data).length >= 8, '快照保留完整状态机数据');
  // 压缩之后再产生新日志，验证“InstallSnapshot + 快照点后增量复制”完整链路
  sim.submitCommand({ type: 'set', key: 'post1', value: 'A' });
  runMs(sim, 500);
  sim.submitCommand({ type: 'set', key: 'post2', value: 'B' });
  runMs(sim, 500);
  sim.setDown(laggard, false);
  runMs(sim, 3500);
  const lag = sim.nodes[laggard];
  assert(!!lag.snapshot, '落后节点通过 InstallSnapshot 获得快照');
  assert(lag.snapshot.lastIncludedIndex === leader.snapshot.lastIncludedIndex,
    '快照点一致');
  const sameState = JSON.stringify(lag.state) === JSON.stringify(leader.state);
  assert(sameState, '快照恢复后状态机与 Leader 一致');
  const lagNode = sim.nodes[laggard];
  const delta = lagNode.log.map((e) => e.index + ':' + e.term).join(',');
  const leaderDelta = leader.log.map((e) => e.index + ':' + e.term).join(',');
  assert(delta === leaderDelta && delta === '10:1,11:1',
    '落后节点先装快照再复制 #10/#11，与 Leader 快照后日志一致：' + delta);
  assert(lagNode.state.post2 === 'B' && lagNode.lastApplied === leader.lastApplied,
    '落后节点状态机完整追平（含快照后增量）');
  const healthyApplied = sim.nodeOrder.every((id) => sim.nodes[id].lastApplied === leader.lastApplied);
  assert(healthyApplied, '所有节点 lastApplied 一致，状态机内容等价');
  assert(sim.nodeOrder.every((id) => sim.nodes[id].commitIndex === leader.commitIndex),
    '所有节点 commitIndex 一致');
}

// ---------- 场景 6：宕机恢复 / 多数派宕机不可提交 ----------
console.log('场景6 多数派不可用时拒绝提交，恢复后继续');
{
  const sim = new Sim({ nodeCount: 3, seed: 53, autoSnapshot: false });
  runMs(sim, 5000);
  const leader = leaderOf(sim);
  const followers = sim.nodeOrder.filter((id) => id !== leader.id);
  sim.setDown(followers[0], true);
  sim.setDown(followers[1], true);
  runMs(sim, 3000);
  assert(!sim.leader(), '多数派宕机后无 Leader（旧 Leader 降级）');
  const r = sim.submitCommand({ type: 'set', key: 'z', value: '1' });
  assert(!r.ok, '无 Leader 时客户端命令被拒绝/重定向');
  sim.setDown(followers[0], false);
  sim.setDown(followers[1], false);
  runMs(sim, 5000);
  assert(!!sim.leader(), '恢复后重新选出 Leader');
}

console.log('\n最终日志抽样（场景5 由 sim 独立创建，此处打印场景6 略）');
if (failures > 0) {
  console.error('\n存在 ' + failures + ' 个失败断言');
  process.exit(1);
} else {
  console.log('\n全部场景通过 ✔');
}
