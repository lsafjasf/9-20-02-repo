// 随机混沌测试：随机宕机/恢复/分区/恢复 + 随机客户端命令，结束后验证一致性
require('../js/raft-config.js');
require('../js/network.js');
require('../js/raft.js');
const { Sim } = require('../js/sim.js');

function mulberry32(seed) {
  return function () {
    let t = (seed += 0x6d2b79f5);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

let failed = 0;
function check(cond, msg) {
  if (!cond) { failed++; console.error('  ✗ ' + msg); }
  else console.log('  ✓ ' + msg);
}

[3, 5, 7].forEach((count, ci) => {
  console.log('混沌测试: ' + count + ' 节点');
  const sim = new Sim({ nodeCount: count, seed: 100 + ci, autoSnapshot: true });
  const rng = mulberry32(900 + ci);
  const run = (ms) => { for (let i = 0; i < ms / 20; i++) sim.step(20); };
  run(4000);

  let seq = 0;
  for (let round = 0; round < 24; round++) {
    const roll = rng();
    if (roll < 0.3) {
      const id = 1 + Math.floor(rng() * count);
      sim.setDown(id, true);
      run(400 + Math.floor(rng() * 1200));
      sim.setDown(id, false);
    } else if (roll < 0.55) {
      // 随机分区，保证至少有一方占多数（隔离 1~少数个）
      const k = 1 + Math.floor(rng() * Math.floor(count / 2));
      const shuffled = sim.nodeOrder.slice().sort(() => rng() - 0.5);
      const iso = shuffled.slice(0, k);
      iso.forEach((id) => sim.network.isolate(id));
      sim.events.push({ seq: sim.nextSeq(), time: sim.time, kind: 'system', nodeId: null, text: '混沌随机隔离 ' + iso.join(',') });
      run(800 + Math.floor(rng() * 2000));
      sim.heal();
    } else if (roll < 0.7) {
      const id = 1 + Math.floor(rng() * count);
      sim.setLatency(id, 100 + Math.floor(rng() * 600));
      run(300);
      sim.setLatency(id, 25);
    }
    seq++;
    sim.submitCommand({ type: 'set', key: 'k' + seq, value: 'v' + seq });
    run(300 + Math.floor(rng() * 500));
  }

  // 长时间稳定恢复
  sim.nodeOrder.forEach((id) => {
    sim.network.setDown(id, false);
    sim.nodes[id].setDown(false, sim.time);
  });
  sim.network.heal();
  run(8000);

  const leaders = sim.nodeOrder.filter((id) => sim.nodes[id].role === 'leader');
  check(leaders.length === 1, '最终恰好 1 个 Leader：' + leaders.join(','));

  const leader = sim.nodes[leaders[0]];
  // 已提交日志必须在所有存活节点上一致（index/term）
  let allCommittedConsistent = true;
  sim.nodeOrder.forEach((id) => {
    const n = sim.nodes[id];
    for (let i = 1; i <= leader.commitIndex; i++) {
      const lt = leader.termAt(i);
      const nt = n.termAt(i);
      if (lt !== nt && !(nt === 0 && i <= n.lastApplied && i <= leader.commitIndex)) {
        allCommittedConsistent = false;
      }
    }
  });
  check(allCommittedConsistent, '所有已提交位置的 term 在各节点一致（安全性）');

  const statesEqual = sim.nodeOrder
    .map((id) => JSON.stringify(sim.nodes[id].state))
    .every((v, _, arr) => v === arr[0]);
  check(statesEqual, '所有节点状态机内容完全一致');

  const appliedEqual = sim.nodeOrder.every(
    (id) => sim.nodes[id].lastApplied === leader.lastApplied
  );
  check(appliedEqual, '所有节点 lastApplied 一致 (' + leader.lastApplied + ')');
});

if (failed) { console.error('\n混沌测试失败 ' + failed); process.exit(1); }
console.log('\n混沌测试全部通过 ✔');
