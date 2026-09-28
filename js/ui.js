// ============================================================
// ui.js  -  渲染与交互层（纯原生 JS）
// ============================================================
(function () {
  'use strict';

  let cluster = null;
  let paused = false;
  let speed = 1;
  let lastTs = null;
  let lastRender = 0;
  let eventCursor = 0;
  let filters = {};

  const $ = function (sel) { return document.querySelector(sel); };
  const grid = $('#nodeGrid');
  const rpcBody = $('#rpcLog');

  // ---------------- 集群初始化 ----------------
  function rebuild() {
    const size = parseInt($('#nodeCount').value, 10);
    const seedInput = $('#seedInput');
    RAFT.setSeed(seedInput.value.trim() === '' ? undefined : seedInput.value.trim());
    RAFT.resetClock();
    cluster = new RAFT.Cluster(size);
    eventCursor = 0;
    rpcBody.innerHTML = '';
    buildCards();
    $('#seedVal').textContent = RAFT.getSeed();
    cluster.logEvent('sys', '集群初始化：' + size + ' 个节点，多数派 = ' + cluster.majority() +
      '，seed = ' + RAFT.getSeed());
  }

  // ---------------- 主循环（真实时间 → 模拟时间） ----------------
  function frame(ts) {
    if (lastTs === null) lastTs = ts;
    const dt = Math.min(100, ts - lastTs);
    lastTs = ts;
    if (!paused) RAFT.tick(dt * speed);
    if (ts - lastRender > 120) {
      lastRender = ts;
      render();
    }
    requestAnimationFrame(frame);
  }

  // ---------------- 顶栏事件 ----------------
  $('#restartBtn').addEventListener('click', function () {
    paused = false;
    $('#pauseBtn').textContent = '⏸ 暂停';
    rebuild();
  });

  $('#nodeCount').addEventListener('change', rebuild);

  $('#speed').addEventListener('change', function (e) { speed = parseFloat(e.target.value); });

  $('#pauseBtn').addEventListener('click', function () {
    paused = !paused;
    this.textContent = paused ? '▶ 继续' : '⏸ 暂停';
  });

  function sendCommand() {
    const input = $('#cmdInput');
    let cmd = input.value.trim();
    if (!cmd) return;
    const m = cmd.match(/^SET\s+(\S+)\s*=\s*(.+)$/i);
    const d = cmd.match(/^DEL\s+(\S+)$/i);
    if (!m && !d) {
      cluster.logEvent('cmd', '无法识别的指令（仅支持 SET k=v / DEL k）：' + cmd, true);
      return;
    }
    cluster.submit(cmd);
  }
  $('#sendBtn').addEventListener('click', sendCommand);
  $('#cmdInput').addEventListener('keydown', function (e) { if (e.key === 'Enter') sendCommand(); });
  document.querySelectorAll('.chip').forEach(function (chip) {
    chip.addEventListener('click', function () { $('#cmdInput').value = chip.dataset.cmd; });
  });

  // ---------------- 故障场景 ----------------
  // 把当前 leader 隔离到少数派分区
  $('#scnSplit').addEventListener('click', function () {
    const lead = cluster.leader();
    if (!lead) { cluster.logEvent('sys', '当前没有 leader，无法裂脑', true); return; }
    clearPartitions();
    const gid = 'A';
    lead.setPartition(gid);
    cluster.logEvent('sys', '裂脑：节点' + (lead.id + 1) + ' 被隔离到少数派分区 A');
  });

  $('#scnHeal').addEventListener('click', clearPartitions);

  function clearPartitions() {
    for (const n of cluster.nodes) n.setPartition(null);
    cluster.logEvent('sys', '所有网络分区已愈合');
  }

  $('#scnKillLeader').addEventListener('click', function () {
    const lead = cluster.leader();
    if (!lead) { cluster.logEvent('sys', '当前没有 leader', true); return; }
    lead.crash();
    RAFT.schedule(2500, function () {
      if (lead.down) lead.recover();
      cluster.logEvent('sys', '节点' + (lead.id + 1) + ' 宕机 2.5s 后自动恢复');
    });
  });

  $('#scnAllDown').addEventListener('click', function () {
    for (const n of cluster.nodes) n.crash();
    cluster.logEvent('sys', '全部节点宕机');
    let delay = 1200;
    for (const n of cluster.nodes) {
      RAFT.schedule(delay, (function (node) {
        return function () { node.recover(); };
      })(n));
      delay += 700;
    }
  });

  $('#clearLog').addEventListener('click', function () {
    rpcBody.innerHTML = '';
    eventCursor = cluster.events.length;
  });

  document.querySelectorAll('#filters input').forEach(function (cb) {
    filters[cb.value] = cb.checked;
    cb.addEventListener('change', function () {
      filters[cb.value] = cb.checked;
      eventCursor = 0;
      rpcBody.innerHTML = '';
      flushEvents();
    });
  });

  // ---------------- 节点卡片骨架 ----------------
  function buildCards() {
    grid.innerHTML = '';
    cluster.nodes.forEach(function (n) {
      const card = document.createElement('div');
      card.className = 'card';
      card.id = 'card-' + n.id;
      card.innerHTML =
        '<div class="card-head">' +
          '<span class="node-name">节点 ' + (n.id + 1) + '</span>' +
          '<span class="role follower" data-f="role">follower</span>' +
          '<span class="head-spacer"></span>' +
          '<span class="badges">' +
            '<span class="badge down" data-f="downBadge" style="display:none">宕机</span>' +
            '<span class="badge part" data-f="partBadge" style="display:none"></span>' +
            '<span class="badge delay" data-f="delayBadge" style="display:none"></span>' +
            '<span class="badge iso" data-f="isoBadge" style="display:none">孤岛 leader</span>' +
          '</span>' +
        '</div>' +
        '<div class="card-body">' +
          '<div class="kv-row"><span>currentTerm</span><b data-f="term"></b></div>' +
          '<div class="kv-row"><span>votedFor</span><b data-f="vote"></b></div>' +
          '<div class="kv-row"><span>commitIndex / lastApplied</span><b data-f="commit"></b></div>' +
          '<div class="kv-row"><span>leaderId</span><b data-f="leader"></b></div>' +
          '<div data-f="peerWrap" style="display:none">' +
            '<div class="section-name">复制进度（nextIndex / matchIndex）</div>' +
            '<table class="peer-table" data-f="peerTable"></table>' +
          '</div>' +
          '<div><div class="section-name">快照（log compaction）</div><div class="snap-tag" data-f="snap"></div></div>' +
          '<div><div class="section-name">状态机（已 apply 的键值）</div><div class="sm-box" data-f="sm"></div></div>' +
          '<div><div class="section-name">日志数组（绿=已提交）</div><div class="log-box"><div class="log-list" data-f="logs"></div></div></div>' +
        '</div>' +
        '<div class="card-actions">' +
          '<button class="act" data-a="crash">宕机</button>' +
          '<label style="flex:1;min-width:64px">分区 ' +
            '<select data-a="partition">' +
              '<option value="">互通</option><option value="A">A 组</option><option value="B">B 组</option>' +
            '</select>' +
          '</label>' +
          '<label style="flex:1;min-width:64px">延迟 ' +
            '<select data-a="delay">' +
              '<option value="0">正常</option><option value="1">慢 200ms</option><option value="2">极慢 800ms</option>' +
            '</select>' +
          '</label>' +
          '<button class="act" data-a="snapshot">生成快照</button>' +
        '</div>';
      grid.appendChild(card);

      card.querySelector('[data-a="crash"]').addEventListener('click', function () {
        if (n.down) n.recover(); else n.crash();
      });
      card.querySelector('[data-a="partition"]').addEventListener('change', function (e) {
        n.setPartition(e.target.value === '' ? null : e.target.value);
      });
      card.querySelector('[data-a="delay"]').addEventListener('change', function (e) {
        n.delay = parseInt(e.target.value, 10);
      });
      card.querySelector('[data-a="snapshot"]').addEventListener('click', function () {
        if (!n.takeSnapshot()) {
          cluster.logEvent('snap', '节点' + (n.id + 1) + ' 暂无可快照内容（lastApplied 未前进）', true);
        }
      });
    });
  }

  function fld(card, name) { return card.querySelector('[data-f="' + name + '"]'); }
  function esc(s) {
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;')
      .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  // ---------------- 渲染单个节点 ----------------
  function renderCard(n) {
    const card = $('#card-' + n.id);

    card.classList.toggle('leader', n.state === 'leader' && !n.down);
    card.classList.toggle('candidate', n.state === 'candidate' && !n.down);
    card.classList.toggle('is-down', n.down);
    card.classList.toggle('partitioned', n.partition !== null);

    const role = fld(card, 'role');
    role.textContent = n.down ? 'down' : n.state;
    role.className = 'role ' + (n.down ? 'follower' : n.state);

    fld(card, 'downBadge').style.display = n.down ? '' : 'none';

    const partBadge = fld(card, 'partBadge');
    if (n.partition !== null) {
      partBadge.style.display = '';
      partBadge.textContent = '分区 ' + n.partition;
    } else partBadge.style.display = 'none';

    const delayBadge = fld(card, 'delayBadge');
    if (n.delay > 0) {
      delayBadge.style.display = '';
      delayBadge.textContent = '延迟 +' + RAFT.CFG.DELAYS[n.delay] + 'ms';
    } else delayBadge.style.display = 'none';

    // leader 能否联系多数派？不能则标记为「孤岛」
    const isolated = n.state === 'leader' && n.reachableCount() < cluster.majority() - 1;
    fld(card, 'isoBadge').style.display = isolated ? '' : 'none';

    fld(card, 'term').textContent = n.currentTerm;
    fld(card, 'vote').textContent = n.votedFor === null ? 'null' : '节点' + (n.votedFor + 1);
    fld(card, 'commit').textContent = n.commitIndex + ' / ' + n.lastApplied;
    fld(card, 'leader').textContent = n.leaderId === null ? '无' : '节点' + (n.leaderId + 1);

    // 快照信息
    const snap = fld(card, 'snap');
    if (n.snapshot) {
      snap.innerHTML = 'lastIncludedIndex=<b>' + n.snapshot.lastIncludedIndex +
        '</b>，term=' + n.snapshot.lastIncludedTerm +
        '，快照内键值 ' + Object.keys(n.snapshot.state).length + ' 项（index ≤ ' +
        n.snapshot.lastIncludedIndex + ' 的日志已压缩）';
    } else {
      snap.textContent = '尚未生成快照';
    }

    // 状态机
    const sm = fld(card, 'sm');
    const keys = Object.keys(n.sm).sort();
    if (!keys.length) {
      sm.innerHTML = '<span class="empty">{ }（空）</span>';
    } else {
      sm.innerHTML = keys.map(function (k) {
        return '<span class="sm-entry"><span class="k">' + esc(k) + '</span>=<span class="v">' +
          esc(n.sm[k]) + '</span></span>';
      }).join('');
    }

    // 日志数组
    const logs = fld(card, 'logs');
    let html = '';
    if (n.snapshot) {
      html += '<span class="log-entry committed noop" title="已被快照压缩">' +
        '<span class="idx">1..' + n.snapshot.lastIncludedIndex + '</span>' +
        '<span class="cmd">快照</span></span>';
    }
    if (!n.log.length && !n.snapshot) html = '<span class="empty">空日志</span>';
    n.log.forEach(function (entry, i) {
      const index = n.firstLogIndex + i;
      const cls = ['log-entry', 't' + (entry.term % 4 + 1)];
      if (index <= n.commitIndex) cls.push('committed');
      if (index <= n.lastApplied) cls.push('applied');
      if (entry.cmd === 'NOOP') cls.push('noop');
      html += '<span class="' + cls.join(' ') + '" title="term=' + entry.term + ' index=' + index + '">' +
        '<span class="idx">#' + index + ' t' + entry.term + '</span>' +
        '<span class="cmd">' + esc(entry.cmd) + '</span></span>';
    });
    logs.innerHTML = html;

    // leader 复制进度表
    const peerWrap = fld(card, 'peerWrap');
    if (n.state === 'leader' && n.nextIndex) {
      peerWrap.style.display = '';
      let rows = '<tr><th>peer</th>';
      for (let i = 0; i < cluster.nodes.length; i++) {
        if (i !== n.id) rows += '<th>节点' + (i + 1) + '</th>';
      }
      rows += '</tr><tr><th>next</th>';
      for (let i = 0; i < cluster.nodes.length; i++) {
        if (i !== n.id) rows += '<td>' + n.nextIndex[i] + '</td>';
      }
      rows += '</tr><tr><th>match</th>';
      for (let i = 0; i < cluster.nodes.length; i++) {
        if (i !== n.id) rows += '<td>' + n.matchIndex[i] + '</td>';
      }
      rows += '</tr>';
      fld(card, 'peerTable').innerHTML = rows;
    } else {
      peerWrap.style.display = 'none';
    }

    // 操作控件回显
    card.querySelector('[data-a="crash"]').textContent = n.down ? '恢复' : '宕机';
    card.querySelector('[data-a="crash"]').classList.toggle('on', n.down);
    card.querySelector('[data-a="partition"]').value = n.partition === null ? '' : n.partition;
    card.querySelector('[data-a="delay"]').value = String(n.delay);
  }

  // ---------------- 顶部状态条 ----------------
  function renderStatus() {
    $('#simTime').textContent = RAFT.currentTime();
    $('#quorumVal').textContent = cluster.majority() + ' 票（共 ' + cluster.size + ' 节点）';

    const lead = cluster.leader();
    let leadText = '选举中…';
    if (lead) {
      leadText = '节点' + (lead.id + 1) + '（term ' + lead.currentTerm + '）';
      if (lead.reachableCount() < cluster.majority() - 1) {
        leadText += ' — 处于少数派分区，无法提交';
      }
    }
    $('#leaderVal').textContent = leadText;

    // 安全检查：同一 term 是否出现过两个不同 leader
    let violation = false;
    Object.keys(cluster.epochs).forEach(function (term) {
      const list = cluster.epochs[term].map(function (e) { return e.id; });
      if (list.some(function (id, i) { return list.indexOf(id) !== i; })) violation = true;
    });
    const safety = $('#safetyVal');
    safety.textContent = violation ? '出现双 leader！!' : '未出现 ✓';
    safety.className = violation ? 'bad' : 'ok';

    // 一致性检查：存活节点已 apply 的状态机是否一致
    const alive = cluster.nodes.filter(function (n) { return !n.down; });
    const sig = alive.map(function (n) {
      return JSON.stringify({ a: n.lastApplied, s: n.sm });
    });
    const consistent = sig.every(function (x) { return x === sig[0]; });
    const c = $('#consistVal');
    c.textContent = consistent ? '一致 ✓' : '收敛中（各节点 apply 进度/状态不同）';
    c.className = consistent ? 'ok' : 'bad';

    $('#pendingVal').textContent = cluster.clientPending;
  }

  // ---------------- RPC/事件流增量渲染 ----------------
  function flushEvents() {
    const frag = document.createDocumentFragment();
    let added = 0;
    while (eventCursor < cluster.events.length) {
      const ev = cluster.events[eventCursor++];
      if (filters[ev.type] === false) continue;
      const line = document.createElement('div');
      line.className = 'rpc-line ' + ev.type + (ev.bad ? ' bad' : '');
      const ms = String(ev.time).padStart(7, ' ');
      line.innerHTML = '<span class="t">[' + ms + 'ms]</span><span class="msg">' + esc(ev.text) + '</span>';
      frag.appendChild(line);
      added++;
    }
    if (added) {
      rpcBody.appendChild(frag);
      // 最多保留 250 行 DOM
      while (rpcBody.childNodes.length > 250) rpcBody.removeChild(rpcBody.firstChild);
      const nearBottom = rpcBody.scrollHeight - rpcBody.scrollTop - rpcBody.clientHeight < 60;
      if (nearBottom) rpcBody.scrollTop = rpcBody.scrollHeight;
    }
  }

  function render() {
    if (!cluster) return;
    cluster.nodes.forEach(renderCard);
    renderStatus();
    flushEvents();
  }

  // ---------------- 启动 ----------------
  rebuild();
  requestAnimationFrame(frame);
})();
