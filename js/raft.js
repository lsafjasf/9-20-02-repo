// ============================================================
// raft.js  -  Raft 共识算法模拟核心（纯原生 JS，无外部依赖）
// 包含：leader election / term+vote / heartbeat / log replication
//       commitIndex / lastApplied / 冲突截断 / snapshot compaction
// ============================================================
(function () {
  'use strict';

  const CFG = {
    HEARTBEAT: 100,              // leader 心跳间隔（模拟毫秒）
    ELECTION_MIN: 250,           // 选举超时下限
    ELECTION_MAX: 500,           // 选举超时上限
    DELAYS: [0, 200, 800],       // 每节点延迟等级：正常 / 慢 / 极慢（单向附加毫秒）
    BATCH: 200,                  // 单次 AppendEntries 携带的最大日志条数
    EVENT_LIMIT: 500             // 事件面板最多保留记录
  };

  // ---------------- 离散事件时钟 ----------------
  let now = 0;
  let queue = [];
  let timerSeq = 1;

  // ---------------- 可注入随机源 ----------------
  // mulberry32：小巧的确定性 PRNG。
  // 默认种子取自 Math.random()（行为与之前一致）；
  // 审计/重放时调用 RAFT.setSeed(seed) 即可让同 seed 的事件序列逐条一致。
  function hashSeed(seed) {
    const s = String(seed);
    let h = 2166136261 >>> 0;
    for (let i = 0; i < s.length; i++) {
      h ^= s.charCodeAt(i);
      h = Math.imul(h, 16777619);
    }
    return h >>> 0;
  }

  let rngState = 0;
  let currentSeed = null;

  function setSeed(seed) {
    if (seed === undefined || seed === null || seed === '') {
      seed = Math.floor(Math.random() * 0xffffffff);
    }
    currentSeed = seed;
    rngState = hashSeed(seed);
  }

  function getSeed() { return currentSeed; }

  function random() {
    rngState = (rngState + 0x6D2B79F5) >>> 0;
    let t = rngState;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  }

  setSeed();  // 默认仍然随机

  function schedule(delayMs, fn) {
    const ev = { id: timerSeq++, time: now + Math.max(0, delayMs), fn: fn, dead: false };
    queue.push(ev);
    return ev;
  }
  function cancel(ev) { if (ev) ev.dead = true; }
  function resetClock() { now = 0; queue = []; }
  function currentTime() { return now; }

  function tick(deltaMs) {
    const horizon = now + deltaMs;
    while (queue.length) {
      let next = null;
      for (const ev of queue) {
        if (ev.dead) continue;
        if (next === null || ev.time < next.time || (ev.time === next.time && ev.id < next.id)) next = ev;
      }
      if (next === null || next.time > horizon) break;
      queue.splice(queue.indexOf(next), 1);
      now = next.time;
      try { next.fn(); } catch (err) { console.error('event error', err); }
    }
    now = horizon;
  }

  function electionTimeout() {
    return CFG.ELECTION_MIN + random() * (CFG.ELECTION_MAX - CFG.ELECTION_MIN);
  }

  // ---------------- 集群 ----------------
  function Cluster(size) {
    this.size = size;
    this.nodes = [];
    this.events = [];          // {time, type, text, bad}
    this.epochs = {};          // term -> [{id, t}] 同一 term 的掌权记录
    this.clientSeq = 0;
    this.clientPending = 0;
    this.build();
  }

  Cluster.prototype.build = function () {
    for (let i = 0; i < this.size; i++) this.nodes.push(new RaftNode(i, this));
    for (const n of this.nodes) n.resetElectionTimer();
  };

  Cluster.prototype.node = function (id) { return this.nodes[id]; };

  Cluster.prototype.majority = function () { return Math.floor(this.size / 2) + 1; };

  Cluster.prototype.leader = function () {
    for (const n of this.nodes) if (!n.down && n.state === 'leader') return n;
    return null;
  };

  Cluster.prototype.logEvent = function (type, text, bad) {
    this.events.push({ time: Math.round(now), type: type, text: text, bad: !!bad });
    if (this.events.length > CFG.EVENT_LIMIT) this.events.shift();
  };

  Cluster.prototype.leaderElected = function (node) {
    const term = node.currentTerm;
    if (!this.epochs[term]) this.epochs[term] = [];
    const list = this.epochs[term];
    if (list.length && list[list.length - 1].id !== node.id) {
      this.logEvent('sys', '!! 安全告警：term ' + term + ' 内出现第二个 leader（不应发生）', true);
    }
    list.push({ id: node.id, t: now });
  };

  Cluster.prototype.canCommunicate = function (a, b) {
    if (a === b) return true;
    const na = this.nodes[a], nb = this.nodes[b];
    if (na.down || nb.down) return false;
    if (na.partition !== null || nb.partition !== null) {
      if (na.partition !== nb.partition) return false;
    }
    return true;
  };

  Cluster.prototype.setPartition = function (id, g) { this.nodes[id].setPartition(g); };

  Cluster.prototype.reachable = function (id) {
    let count = 0;
    for (const n of this.nodes) if (n.id !== id && this.canCommunicate(id, n.id)) count++;
    return count;
  };

  // 客户端请求：路由给 leader；无 leader 时稍后重试
  Cluster.prototype.submit = function (command) {
    const self = this;
    this.clientSeq++;
    const cmd = typeof command === 'string' ? command : command;
    this.clientPending++;
    let tries = 0;

    function attempt() {
      tries++;
      const lead = self.leader();
      if (lead && self.reachable(lead.id) >= self.majority() - 1) {
        self.clientPending--;
        lead.appendClient(cmd);
        self.logEvent('cmd', '客户端 → 节点' + (lead.id + 1) + '：' + cmd);
        return;
      }
      if (tries >= 60) {
        self.clientPending--;
        self.logEvent('cmd', '请求失败（无可用 leader）：' + cmd, true);
        return;
      }
      schedule(100, attempt);
    }
    attempt();
  };

  // ---------------- 网络层 ----------------
  Cluster.prototype.rpc = function (from, to, rpc, payload, done) {
    const self = this;
    if (!this.canCommunicate(from, to)) {
      const tag = this.nodes[from].down || this.nodes[to].down ? '宕机丢弃' : '分区丢弃';
      this.logEvent('drop', rpcName(rpc) + ' 节点' + (from + 1) + '→节点' + (to + 1) + '（' + tag + '）');
      if (done) done(null);
      return;
    }
    const sender = this.nodes[from], receiver = this.nodes[to];
    const oneWay = CFG.DELAYS[sender.delay] + CFG.DELAYS[receiver.delay] + 2;
    this.logEvent(rpc, rpcName(rpc) + ' 请求 节点' + (from + 1) + '→节点' + (to + 1) + rpcBrief(rpc, payload));

    schedule(oneWay, function () {
      if (!self.canCommunicate(from, to)) {
        self.logEvent('drop', rpcName(rpc) + ' 节点' + (from + 1) + '→节点' + (to + 1) + '（途中中断）');
        if (done) done(null);
        return;
      }
      let reply;
      try { reply = receiver.onRpc(rpc, payload, from); }
      catch (e) { console.error(e); reply = null; }
      schedule(oneWay, function () {
        if (!self.canCommunicate(from, to)) {
          self.logEvent('drop', rpcName(rpc) + ' 回复 节点' + (to + 1) + '→节点' + (from + 1) + '（途中中断）');
          if (done) done(null);
          return;
        }
        if (reply !== undefined && reply !== null && done) done(reply);
      });
    });
  };

  function rpcName(rpc) {
    return rpc === 'vote' ? 'RequestVote' : rpc === 'app' ? 'AppendEntries' : 'InstallSnapshot';
  }

  function rpcBrief(rpc, p) {
    if (rpc === 'vote') return ' term=' + p.term + ' lastLog=(' + p.lastLogIndex + ',' + p.lastLogTerm + ')';
    if (rpc === 'snap') return ' term=' + p.term + ' lastIncludedIndex=' + p.lastIncludedIndex;
    let s = ' term=' + p.term + ' prev=(' + p.prevLogIndex + ',' + p.prevLogTerm + ')' +
      ' commit=' + p.leaderCommit;
    if (p.entries && p.entries.length) s += ' entries=' + p.entries.length;
    return s;
  }

  // ---------------- Raft 节点 ----------------
  function RaftNode(id, cluster) {
    this.id = id;
    this.cluster = cluster;

    // 持久化状态
    this.currentTerm = 0;
    this.votedFor = null;
    this.log = [];                 // index = firstLogIndex + i
    this.firstLogIndex = 1;
    this.firstLogTerm = 0;

    // snapshot
    this.snapshot = null;          // {lastIncludedIndex,lastIncludedTerm,state}
    this.installPending = {};      // peer id -> lastIncludedIndex（节流 InstallSnapshot）

    // 易失状态
    this.state = 'follower';
    this.leaderId = null;
    this.commitIndex = 0;
    this.lastApplied = 0;
    this.sm = {};                  // 状态机（键值）

    // leader 易失状态
    this.nextIndex = null;
    this.matchIndex = null;

    // 故障注入
    this.down = false;
    this.partition = null;        // 分区组 id
    this.delay = 0;

    this.electionTimer = null;
    this.heartbeatTimer = null;
  }

  Object.defineProperty(RaftNode.prototype, 'lastLogIndex', {
    get: function () { return this.firstLogIndex - 1 + this.log.length; }
  });

  RaftNode.prototype.termAt = function (index) {
    if (index < 1) return 0;
    if (this.snapshot && index === this.snapshot.lastIncludedIndex) return this.snapshot.lastIncludedTerm;
    const pos = index - this.firstLogIndex;
    if (pos >= 0 && pos < this.log.length) return this.log[pos].term;
    if (this.snapshot && index < this.snapshot.lastIncludedIndex) return this.snapshot.lastIncludedTerm;
    return 0;
  };

  RaftNode.prototype.entryAt = function (index) {
    const pos = index - this.firstLogIndex;
    if (pos >= 0 && pos < this.log.length) return this.log[pos];
    if (this.snapshot && index <= this.snapshot.lastIncludedIndex) {
      return { term: this.snapshot.lastIncludedTerm, cmd: '(已被快照覆盖)' };
    }
    return null;
  };

  // ---------------- 选举 ----------------
  RaftNode.prototype.resetElectionTimer = function () {
    cancel(this.electionTimer);
    if (this.down || this.state === 'leader') return;
    const self = this;
    this.electionTimer = schedule(electionTimeout(), function () { self.startElection(); });
  };

  RaftNode.prototype.becomeFollower = function (term, leaderId) {
    this.state = 'follower';
    if (term > this.currentTerm) this.votedFor = null;
    this.currentTerm = term;
    if (leaderId !== undefined && leaderId !== null) this.leaderId = leaderId;
    cancel(this.heartbeatTimer);
    this.resetElectionTimer();
  };

  RaftNode.prototype.startElection = function () {
    if (this.down) return;
    this.state = 'candidate';
    this.currentTerm++;
    this.votedFor = this.id;
    this.leaderId = null;
    cancel(this.heartbeatTimer);
    const self = this;
    const term = this.currentTerm;
    const lastIdx = this.lastLogIndex;
    const lastTerm = this.termAt(lastIdx);

    let granted = 1;
    let needed = this.cluster.majority();

    this.cluster.logEvent('elec', '节点' + (this.id + 1) + ' 发起选举（term=' + term + '）');
    if (granted >= needed) { this.becomeLeader(); return; }

    for (const peer of this.cluster.nodes) {
      if (peer.id === this.id) continue;
      this.cluster.rpc(this.id, peer.id, 'vote', {
        term: term, candidateId: this.id,
        lastLogIndex: lastIdx, lastLogTerm: lastTerm
      }, function (reply) {
        if (!reply || self.down) return;
        if (reply.term > self.currentTerm) { self.becomeFollower(reply.term); return; }
        if (self.state !== 'candidate' || self.currentTerm !== term) return;
        if (reply.voteGranted) {
          granted++;
          if (granted === needed) self.becomeLeader();
        }
      });
    }
    this.resetElectionTimer();
  };

  // RequestVote 处理
  RaftNode.prototype.onRequestVote = function (args) {
    const { term, candidateId, lastLogIndex, lastLogTerm } = args;
    if (term < this.currentTerm) {
      return { term: this.currentTerm, voteGranted: false };
    }
    if (term > this.currentTerm) this.becomeFollower(term);

    const myLastTerm = this.termAt(this.lastLogIndex);
    const logUpToDate =
      lastLogTerm > myLastTerm ||
      (lastLogTerm === myLastTerm && lastLogIndex >= this.lastLogIndex);

    const canVote = (this.votedFor === null || this.votedFor === candidateId) && logUpToDate;
    if (canVote) {
      this.votedFor = candidateId;
      this.resetElectionTimer();
    }
    return { term: this.currentTerm, voteGranted: canVote };
  };

  // ---------------- Leader / 心跳 / 复制 ----------------
  RaftNode.prototype.becomeLeader = function () {
    if (this.down) return;
    this.state = 'leader';
    this.leaderId = this.id;
    const n = this.cluster.nodes.length;
    this.nextIndex = new Array(n).fill(this.lastLogIndex + 1);
    this.matchIndex = new Array(n).fill(0);
    cancel(this.electionTimer);
    this.cluster.leaderElected(this);
    this.cluster.logEvent('elec', '★ 节点' + (this.id + 1) + ' 成为 leader（term=' + this.currentTerm + '）');
    // 新 leader 提交一条 NOOP，用于提交前任 term 的日志
    this.log.push({ term: this.currentTerm, cmd: 'NOOP' });
    this.broadcastAppend();
    const self = this;
    (function beat() {
      self.heartbeatTimer = schedule(CFG.HEARTBEAT, function () {
        if (!self.down && self.state === 'leader') {
          self.broadcastAppend();
          beat();
        }
      });
    })();
  };

  RaftNode.prototype.appendClient = function (cmd) {
    if (this.state !== 'leader') return;
    this.log.push({ term: this.currentTerm, cmd: cmd });
    this.broadcastAppend();
  };

  RaftNode.prototype.broadcastAppend = function () {
    for (const peer of this.cluster.nodes) {
      if (peer.id !== this.id) this.sendAppend(peer.id);
    }
  };

  RaftNode.prototype.sendAppend = function (peerId) {
    const self = this;
    const term = this.currentTerm;

    // follower 落后过多 → 发 InstallSnapshot
    if (this.snapshot && this.nextIndex[peerId] <= this.snapshot.lastIncludedIndex) {
      const li = this.snapshot.lastIncludedIndex;
      if (this.installPending[peerId] === li) return;
      this.installPending[peerId] = li;
      this.cluster.rpc(this.id, peerId, 'snap', {
        term: term, leaderId: this.id,
        lastIncludedIndex: this.snapshot.lastIncludedIndex,
        lastIncludedTerm: this.snapshot.lastIncludedTerm,
        data: JSON.parse(JSON.stringify(this.snapshot.state))
      }, function (reply) {
        self.installPending[peerId] = null;
        if (!reply) return;
        self.onSnapshotReply(peerId, term, li, reply);
      });
      return;
    }

    const prevIndex = this.nextIndex[peerId] - 1;
    const fromPos = this.nextIndex[peerId] - this.firstLogIndex;
    let entries = [];
    if (fromPos >= 0) {
      entries = this.log.slice(Math.max(0, fromPos), Math.max(0, fromPos) + CFG.BATCH);
    }
    const payload = {
      term: term, leaderId: this.id,
      prevLogIndex: prevIndex,
      prevLogTerm: this.termAt(prevIndex),
      entries: JSON.parse(JSON.stringify(entries)),
      leaderCommit: this.commitIndex
    };
    this.cluster.rpc(this.id, peerId, 'app', payload, function (reply) {
      if (!reply || self.down) return;
      self.onAppendReply(peerId, term, payload, reply);
    });
  };

  // AppendEntries 接收方：校验 + 冲突截断 + 强制覆盖
  RaftNode.prototype.onAppendEntries = function (args) {
    const { term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit } = args;
    if (term < this.currentTerm) {
      return { term: this.currentTerm, success: false, conflictIndex: this.lastLogIndex + 1 };
    }
    if (term >= this.currentTerm) this.becomeFollower(term, leaderId);

    // 快照分界点之前的内容不校验
    const snapIdx = this.snapshot ? this.snapshot.lastIncludedIndex : 0;

    if (prevLogIndex < snapIdx) {
      // leader 视图还停在旧快照前：若其包含的最后位置也已快照，告知直接快进
      if (prevLogIndex + entries.length <= snapIdx) {
        return { term: this.currentTerm, success: true, xindex: snapIdx + 1 };
      }
    } else if (prevLogIndex >= snapIdx) {
      if (prevLogIndex > this.lastLogIndex) {
        return { term: this.currentTerm, success: false, conflictIndex: this.lastLogIndex + 1 };
      }
      if (this.termAt(prevLogIndex) !== prevLogTerm) {
        const conflictTerm = this.termAt(prevLogIndex);
        let conflictIndex = prevLogIndex;
        while (conflictIndex > this.firstLogIndex &&
               this.termAt(conflictIndex - 1) === conflictTerm) {
          conflictIndex--;
        }
        return { term: this.currentTerm, success: false,
                 conflictIndex: conflictIndex, conflictTerm: conflictTerm };
      }
    }

    // 合并 / 覆盖日志（跳过已被本地快照覆盖的部分）
    for (let k = 0; k < entries.length; k++) {
      const index = prevLogIndex + 1 + k;
      if (index <= snapIdx) continue;
      const entry = entries[k];
      if (index <= this.lastLogIndex) {
        if (this.termAt(index) !== entry.term) {
          // 冲突：截断 index 及之后的全部日志（强制覆盖）
          const cutPos = index - this.firstLogIndex;
          if (cutPos >= 0) {
            this.log.length = cutPos;
            this.cluster.logEvent('log', '节点' + (this.id + 1) +
              ' 在 index=' + index + ' 与 leader 冲突，截断日志（term ' +
              this.termAt(index) + '→' + entry.term + '）');
            this.log.push(entry);
          }
        }
      } else {
        this.log.push(entry);
      }
    }

    if (leaderCommit > this.commitIndex) {
      this.commitIndex = Math.min(leaderCommit, this.lastLogIndex);
      this.applyAll();
    }
    return { term: this.currentTerm, success: true, xindex: this.lastLogIndex + 1 };
  };


  RaftNode.prototype.onAppendReply = function (peerId, term, payload, reply) {
    if (this.state !== 'leader' || term !== this.currentTerm) return;
    if (reply.term > this.currentTerm) { this.becomeFollower(reply.term); return; }

    if (reply.success) {
      this.matchIndex[peerId] = Math.max(this.matchIndex[peerId], reply.xindex - 1);
      this.nextIndex[peerId] = this.matchIndex[peerId] + 1;
      this.advanceCommit();
      // 还有未发完的日志，立即继续推送
      if (this.nextIndex[peerId] <= this.lastLogIndex) this.sendAppend(peerId);
      return;
    }

    // 拒绝：冲突回溯（优先使用 follower 提示）
    let ni;
    const ci = reply.conflictIndex;
    if (ci !== undefined && reply.conflictTerm !== undefined) {
      ni = ci;
      const lastOfTerm = this.lastIndexOfTerm(reply.conflictTerm);
      if (lastOfTerm > 0) ni = lastOfTerm + 1;
    } else if (ci !== undefined) {
      ni = ci;
    } else {
      ni = this.nextIndex[peerId] - 1;
    }
    this.nextIndex[peerId] = Math.max(1, Math.min(ni, this.nextIndex[peerId] - 1));
    this.sendAppend(peerId);
  };

  RaftNode.prototype.lastIndexOfTerm = function (term) {
    if (this.snapshot && this.snapshot.lastIncludedTerm === term) return this.snapshot.lastIncludedIndex;
    for (let i = this.log.length - 1; i >= 0; i--) {
      if (this.log[i].term === term) return this.firstLogIndex + i;
    }
    return 0;
  };

  // 推进 commitIndex（仅提交当前 leader term，再间接提交旧 term）
  RaftNode.prototype.advanceCommit = function () {
    const n = this.cluster.nodes.length;
    for (let N = this.lastLogIndex; N > this.commitIndex; N--) {
      if (this.termAt(N) !== this.currentTerm) continue;
      let rep = 1;
      for (let i = 0; i < n; i++) {
        if (i !== this.id && (this.matchIndex[i] || 0) >= N) rep++;
      }
      if (rep >= this.cluster.majority()) {
        this.commitIndex = N;
        this.applyAll();
        return;
      }
    }
  };

  RaftNode.prototype.applyAll = function () {
    while (this.lastApplied < this.commitIndex) {
      const idx = this.lastApplied + 1;
      const snapIdx = this.snapshot ? this.snapshot.lastIncludedIndex : 0;
      if (idx <= snapIdx) { this.lastApplied = snapIdx; continue; }
      const entry = this.entryAt(idx);
      if (!entry) break;
      this.applyCommand(entry.cmd);
      this.lastApplied = idx;
    }
  };

  // 键值状态机：支持 SET x=1 / DEL x / NOOP
  RaftNode.prototype.applyCommand = function (cmd) {
    if (!cmd || cmd === 'NOOP') return;
    const m = cmd.match(/^SET\s+(\S+)\s*=\s*(.+)$/i);
    const d = cmd.match(/^DEL\s+(\S+)$/i);
    if (m) { this.sm[m[1]] = m[2]; }
    else if (d) { delete this.sm[d[1]]; }
  };

  // ---------------- Snapshot / Log Compaction ----------------
  RaftNode.prototype.takeSnapshot = function () {
    if (this.down) return false;
    this.applyAll();
    const idx = this.lastApplied;
    if (idx <= 0) return false;
    const oldIdx = this.snapshot ? this.snapshot.lastIncludedIndex : 0;
    if (idx <= oldIdx) return false;

    this.snapshot = {
      lastIncludedIndex: idx,
      lastIncludedTerm: this.termAt(idx),
      state: JSON.parse(JSON.stringify(this.sm))
    };
    // 压缩：丢弃已包含在快照中的日志
    const cutPos = idx - this.firstLogIndex + 1;
    if (cutPos > 0 && cutPos <= this.log.length) {
      this.log = this.log.slice(cutPos);
      this.firstLogIndex = idx + 1;
      this.firstLogTerm = this.termAt(idx + 1) || 0;
    }
    this.cluster.logEvent('snap', '节点' + (this.id + 1) + ' 生成快照 @index=' + idx +
      '，压缩 ' + cutPos + ' 条日志');
    return true;
  };

  RaftNode.prototype.onInstallSnapshot = function (args) {
    const { term, leaderId, lastIncludedIndex, lastIncludedTerm, data } = args;
    if (term < this.currentTerm) return { term: this.currentTerm, success: false };
    this.becomeFollower(term, leaderId);
    if (this.snapshot && lastIncludedIndex <= this.snapshot.lastIncludedIndex) {
      return { term: this.currentTerm, success: true };
    }
    // 丢弃快照点之前/之处的日志，强制采用 leader 快照
    const keepPos = lastIncludedIndex + 1 - this.firstLogIndex;
    this.log = keepPos > 0 ? this.log.slice(keepPos) : [];
    this.firstLogIndex = lastIncludedIndex + 1;
    this.firstLogTerm = 0;
    this.snapshot = {
      lastIncludedIndex: lastIncludedIndex,
      lastIncludedTerm: lastIncludedTerm,
      state: JSON.parse(JSON.stringify(data))
    };
    this.sm = JSON.parse(JSON.stringify(data));
    if (this.commitIndex < lastIncludedIndex) this.commitIndex = lastIncludedIndex;
    if (this.lastApplied < lastIncludedIndex) this.lastApplied = lastIncludedIndex;
    this.cluster.logEvent('snap', '节点' + (this.id + 1) + ' 安装 leader 快照 @index=' +
      lastIncludedIndex + '（强制覆盖状态机）');
    return { term: this.currentTerm, success: true };
  };

  RaftNode.prototype.onSnapshotReply = function (peerId, term, lastIncludedIndex, reply) {
    if (this.state !== 'leader' || term !== this.currentTerm) return;
    if (reply.term > this.currentTerm) { this.becomeFollower(reply.term); return; }
    if (reply.success) {
      this.matchIndex[peerId] = Math.max(this.matchIndex[peerId] || 0, lastIncludedIndex);
      this.nextIndex[peerId] = Math.max(this.nextIndex[peerId] || 1, lastIncludedIndex + 1);
      this.advanceCommit();
      this.sendAppend(peerId);
    }
  };

  // ---------------- RPC 分发 ----------------
  RaftNode.prototype.onRpc = function (rpc, args, fromId) {
    if (rpc === 'vote') return this.onRequestVote(args);
    if (rpc === 'app') return this.onAppendEntries(args);
    if (rpc === 'snap') return this.onInstallSnapshot(args);
    return null;
  };

  // ---------------- 故障注入 ----------------
  RaftNode.prototype.crash = function () {
    if (this.down) return;
    this.down = true;
    cancel(this.electionTimer);
    cancel(this.heartbeatTimer);
    this.cluster.logEvent('sys', '节点' + (this.id + 1) + ' 宕机（持久化 term/log/snapshot 保留）');
  };

  RaftNode.prototype.recover = function () {
    if (!this.down) return;
    this.down = false;
    this.state = 'follower';
    this.leaderId = null;
    this.nextIndex = null;
    this.matchIndex = null;
    this.installPending = {};
    this.cluster.logEvent('sys', '节点' + (this.id + 1) + ' 恢复，以 follower 身份重新加入');
    this.resetElectionTimer();
  };

  RaftNode.prototype.setPartition = function (g) {
    this.partition = g;
  };

  // 只读：本节点在当前分区中能联系到多少节点
  RaftNode.prototype.reachableCount = function () {
    return this.cluster.reachable(this.id);
  };

  // 导出
  window.RAFT = {
    CFG: CFG,
    Cluster: Cluster,
    RaftNode: RaftNode,
    schedule: schedule,
    tick: tick,
    resetClock: resetClock,
    currentTime: currentTime,
    setSeed: setSeed,
    getSeed: getSeed,
    random: random
  };
})();

