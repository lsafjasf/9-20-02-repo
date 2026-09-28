// 界面渲染与交互：订阅 Sim 变化，渲染节点卡片、RPC、事件，并处理控制按钮。
(function () {
  const { esc } = window.RAFT_COMMON;
  const $ = (id) => document.getElementById(id);

  const sim = new window.Sim({ nodeCount: 5, speed: 1, autoSnapshot: true });
  let selectedId = 1;

  const RPC_LABELS = {
    RequestVote: 'RequestVote 拉票',
    RequestVoteResp: 'RequestVoteResp 投票',
    AppendEntries: 'AppendEntries 复制/心跳',
    AppendEntriesResp: 'AppendEntriesResp 结果',
    InstallSnapshot: 'InstallSnapshot 安装快照',
    InstallSnapshotResp: 'InstallSnapshotResp 快照确认',
  };

  function commandText(command) {
    if (!command) return '';
    if (command.type === 'noop') return 'NOOP';
    if (command.type === 'set') return `SET ${command.key}=${command.value}`;
    if (command.type === 'delete') return `DEL ${command.key}`;
    return JSON.stringify(command);
  }

  function renderStatusStrip() {
    const leader = sim.leader();
    const terms = sim.nodeOrder.map((id) => sim.nodes[id].currentTerm);
    const maxTerm = Math.max.apply(null, terms);
    const downCount = sim.nodeOrder.filter((id) => sim.network.down.has(id)).length;
    const groupIds = new Set();
    sim.nodeOrder.forEach((id) => groupIds.add(sim.network.groups.get(id)));
    const partitioned = groupIds.size > 1;
    const isoCount = partitioned
      ? sim.nodeOrder.filter((id) => sim.network.groups.get(id) !== 0).length
      : 0;
    const committed = leader ? leader.commitIndex : '-';
    $('statusStrip').innerHTML = [
      `<span class="stat">仿真时间 <b>${sim.time} ms</b></span>`,
      `<span class="stat">状态 <b>${sim.running ? '运行中' : '已暂停'}</b></span>`,
      `<span class="stat">当前最高 Term <b class="term-pill">${maxTerm}</b></span>`,
      `<span class="stat">Leader <b style="color:var(--leader)">${leader ? '节点 ' + leader.id : '无（不可多数派决策）'}</b></span>`,
      `<span class="stat">Leader commitIndex <b>${committed}</b></span>`,
      `<span class="stat">宕机 <b>${downCount}</b> / ${sim.nodeOrder.length}</span>`,
      `<span class="stat">分区隔离 <b>${isoCount}</b></span>`,
    ].join('');
  }

  function renderNodeCard(id) {
    const node = sim.nodes[id];
    const down = sim.network.down.has(id);
    const isolated = !down && sim.nodeOrder.some((other) => {
      if (other === id) return false;
      return sim.network.groups.get(other) !== sim.network.groups.get(id);
    });
    const leaderTerm = sim.leader() ? sim.leader().currentTerm : null;
    const voteText = node.votedFor == null ? '—' : 'N' + node.votedFor;
    const latency = sim.network.getLatency(id);

    const chips = [];
    if (node.snapshot) {
      chips.push(
        `<span class="snap-chip" title="快照最后包含 index ${node.snapshot.lastIncludedIndex} / term ${node.snapshot.lastIncludedTerm}">` +
        `快照 ≤ #${node.snapshot.lastIncludedIndex} (t${node.snapshot.lastIncludedTerm})</span>`
      );
    }
    node.log.forEach((entry) => {
      const committed = entry.index <= node.commitIndex;
      const classes = ['log-chip'];
      if (committed) classes.push('committed');
      if (entry.term === node.currentTerm) classes.push('current-term');
      if (entry.command && entry.command.type === 'noop') classes.push('noop');
      const title = `index ${entry.index} / term ${entry.term} / ${esc(commandText(entry.command))}` +
        (committed ? ' / 已提交' : ' / 未提交');
      chips.push(
        `<span class="${classes.join(' ')}" title="${title}">#${entry.index} t${entry.term}` +
        `<span class="cmd">${esc(commandText(entry.command))}</span></span>`
      );
    });

    const stateEntries = Object.keys(node.state).sort();
    const stateHtml = stateEntries.length
      ? stateEntries.map((k) => `<span>${esc(k)}: <b>${esc(node.state[k])}</b></span>`).join('&nbsp;&nbsp;')
      : '<span class="empty">（空状态机）</span>';

    return `
      <div class="node-card ${down ? 'down' : node.role} ${isolated ? 'isolated' : ''} ${selectedId === id ? 'selected' : ''}" data-id="${id}">
        <div class="node-head">
          <span class="node-id">节点 ${id}</span>
          <span class="role-badge ${node.role}">${down ? 'DOWN' : node.role}</span>
          <span class="node-reason" title="${esc(node.reason)}">${esc(node.reason)}</span>
        </div>
        <div class="kv-grid">
          <div class="kv"><span class="k">term</span><span class="v">${node.currentTerm}</span></div>
          <div class="kv"><span class="k">votedFor</span><span class="v">${voteText}</span></div>
          <div class="kv"><span class="k">commitIndex</span><span class="v">${node.commitIndex}</span></div>
          <div class="kv"><span class="k">lastApplied</span><span class="v">${node.lastApplied}</span></div>
        </div>
        <div class="sub-title"><span>日志（蓝底=已提交，绿框=当前 term）</span><span>链路延迟 ${latency}ms</span></div>
        <div class="log-row">${chips.join('') || '<span class="log-chip">空</span>'}</div>
        <div class="sub-title"><span>状态机</span><span>${node.role === 'leader' ? 'Leader' : leaderTerm != null ? '已知 Leader term ' + leaderTerm : ''}</span></div>
        <div class="state-box">${stateHtml}</div>
      </div>`;
  }

  function renderNodes() {
    $('nodes').innerHTML = sim.nodeOrder.map(renderNodeCard).join('');
  }

  function rpcClass(entry) {
    if (entry.status === 'dropped') return 'dropped';
    if (entry.status === 'reply') return entry.success === false ? 'reply fail' : 'reply ok';
    return 'sent';
  }

  function renderRpcLog() {
    const list = sim.rpcs.slice(-80).reverse();
    $('rpcLog').innerHTML = list.map((entry) => `
      <div class="rpc-item ${rpcClass(entry)}">
        <div class="rpc-head">
          <span class="rpc-type">${esc(RPC_LABELS[entry.type] || entry.type)}</span>
          <span class="rpc-meta">${entry.direction} · t${entry.term} · ${entry.time}ms · ${
            entry.status === 'dropped' ? '丢失' : entry.status === 'reply' ? (entry.success === false ? '拒绝' : '成功') : '发出'
          }</span>
        </div>
        ${entry.detail ? `<div class="rpc-detail">${esc(entry.detail)}</div>` : ''}
      </div>`).join('');
  }

  function renderEventLog() {
    const list = sim.events.slice(-60).reverse();
    $('eventLog').innerHTML = list.map((e) => `
      <div class="event-item kind-${e.kind}">
        <span class="time">${e.time}ms${e.nodeId != null ? ' N' + e.nodeId : ''}</span>${esc(e.text)}
      </div>`).join('');
  }

  function render() {
    renderStatusStrip();
    renderNodes();
    renderRpcLog();
    renderEventLog();
  }

  // ---------- 交互 ----------
  function refreshLatencyInput() {
    $('nodeLatency').value = sim.network.getLatency(selectedId);
  }

  function bind() {
    $('nodes').addEventListener('click', (e) => {
      const card = e.target.closest('.node-card');
      if (!card) return;
      selectedId = Number(card.dataset.id);
      $('selectedHint').textContent = '已选中：节点 ' + selectedId;
      refreshLatencyInput();
      renderNodes();
    });

    $('btnPause').addEventListener('click', () => {
      if (sim.running) { sim.pause(); $('btnPause').textContent = '继续'; }
      else { sim.start(); $('btnPause').textContent = '暂停'; }
      renderStatusStrip();
    });

    $('btnReset').addEventListener('click', () => {
      const count = Number($('nodeCount').value);
      sim.pause();
      sim.setNodeCount(count);
      sim.autoSnapshot = $('autoSnapshot').checked;
      selectedId = 1;
      $('selectedHint').textContent = '已选中：节点 1';
      refreshLatencyInput();
      sim.start();
    });

    $('nodeCount').addEventListener('change', () => {
      sim.pause();
      sim.setNodeCount(Number($('nodeCount').value));
      selectedId = 1;
      $('btnPause').textContent = '暂停';
      refreshLatencyInput();
      sim.start();
    });

    $('speed').addEventListener('change', () => {
      sim.speed = Number($('speed').value);
    });
    $('autoSnapshot').addEventListener('change', () => {
      sim.autoSnapshot = $('autoSnapshot').checked;
    });

    $('btnSet').addEventListener('click', () => submit('set'));
    $('btnDelete').addEventListener('click', () => submit('delete'));
    function submit(type) {
      const key = $('cmdKey').value.trim();
      if (!key) { flashHint('请输入 key'); return; }
      const command = type === 'set'
        ? { type: 'set', key, value: $('cmdValue').value }
        : { type: 'delete', key };
      const result = sim.submitCommand(command);
      if (result.ok && result.pending)
        flashHint(`已受理为日志 #${result.index}，等待多数派提交确认（结果见事件流）`);
      else if (!result.ok && result.redirect == null) flashHint(result.error || '提交失败');
      else if (!result.ok) flashHint('当前节点不是 Leader，已尝试转发（Leader: ' + (result.redirect == null ? '未知' : result.redirect) + '）');
    }

    $('btnSplitMinority').addEventListener('click', () => sim.partitionMinority());
    $('btnSplitHalf').addEventListener('click', () => {
      const n = sim.nodeOrder.length;
      const half = Math.floor(n / 2);
      const a = sim.nodeOrder.slice(0, half);
      const b = sim.nodeOrder.slice(half);
      sim.partitionCustom([a, b]);
    });
    $('btnHeal').addEventListener('click', () => sim.heal());

    $('btnKill').addEventListener('click', () => sim.setDown(selectedId, true));
    $('btnRestart').addEventListener('click', () => sim.setDown(selectedId, false));
    $('btnIsolate').addEventListener('click', () => sim.isolate(selectedId));
    $('btnUnisolate').addEventListener('click', () => sim.heal());
    $('btnApplyLatency').addEventListener('click', () => {
      sim.setLatency(selectedId, Number($('nodeLatency').value) || 0);
    });
    $('btnCompact').addEventListener('click', () => {
      const result = sim.compactNode(selectedId);
      if (!result.ok) flashHint(result.error);
    });
    $('btnClearRpc').addEventListener('click', () => {
      sim.clearRpcs();
      renderRpcLog();
    });

    document.addEventListener('keydown', (e) => {
      if (e.target.tagName === 'INPUT') return;
      if (e.key === ' ') {
        e.preventDefault();
        $('btnPause').click();
      }
    });
  }

  let hintTimer = null;
  function flashHint(text) {
    let el = document.getElementById('flashHint');
    if (!el) {
      el = document.createElement('div');
      el.id = 'flashHint';
      el.style.cssText =
        'position:fixed;left:50%;bottom:24px;transform:translateX(-50%);' +
        'background:#3a2026;border:1px solid var(--danger);color:#ffd7dc;' +
        'padding:8px 16px;border-radius:10px;font-size:13px;z-index:50;' +
        'box-shadow:0 8px 30px rgba(0,0,0,.5);opacity:0;transition:opacity .2s;';
      document.body.appendChild(el);
    }
    el.textContent = text;
    el.style.opacity = '1';
    clearTimeout(hintTimer);
    hintTimer = setTimeout(() => { el.style.opacity = '0'; }, 2200);
  }

  // 节点卡片随每次仿真 tick 重绘；日志面板按需追加（这里直接整体重绘，规模足够小）
  document.addEventListener('DOMContentLoaded', () => {
    bind();
    $('selectedHint').textContent = '已选中：节点 1';
    refreshLatencyInput();
    sim.onChange(render);
    render();
    sim.start();
  });
})();
