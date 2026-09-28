// Raft 节点状态机：Leader Election / Log Replication / Committing / Snapshot
(function (global) {
  const {
    RAFT_CONFIG,
    majorityOf,
    randElectionTimeout,
    clone,
  } = global.RAFT_COMMON;

  let ENTRY_SEQ = 0;

  function makeEntry(term, command) {
    ENTRY_SEQ += 1;
    return { uid: 'e' + ENTRY_SEQ, term, command: clone(command) };
  }

  class RaftNode {
    constructor(id, peers, bus, rng) {
      this.id = id;
      this.peers = peers.slice();
      this.bus = bus;
      this.rng = rng || Math.random;
      this.reset();
    }

    reset() {
      // 持久化状态（本演示中宕机不清空；若要模拟磁盘丢失可手动重置）
      this.currentTerm = 0;
      this.votedFor = null;
      this.log = [];
      this.snapshot = null; // { lastIncludedIndex, lastIncludedTerm, data }

      // 易失状态
      this.role = 'follower';
      this.commitIndex = 0;
      this.lastApplied = 0;
      this.state = {};

      // Leader 易失状态
      this.nextIndex = {};
      this.matchIndex = {};

      // 选举计时
      this.electionDeadline = 0;
      this.leaderId = null;

      // Leader 计时
      this.lastHeartbeatSent = 0;
      this.lastContact = {};

      this.down = false;
      this.reason = '初始化为 Follower';
      this.electionStartTerm = 0;
    }

    // ---------- 日志索引工具 ----------
    get lastLogIndex() {
      if (this.log.length) return this.log[this.log.length - 1].index;
      return this.snapshot ? this.snapshot.lastIncludedIndex : 0;
    }

    get lastLogTerm() {
      if (this.log.length) return this.log[this.log.length - 1].term;
      return this.snapshot ? this.snapshot.lastIncludedTerm : 0;
    }

    get firstLogIndex() {
      return this.snapshot ? this.snapshot.lastIncludedIndex + 1 : 1;
    }

    termAt(index) {
      if (this.snapshot && index === this.snapshot.lastIncludedIndex) {
        return this.snapshot.lastIncludedTerm;
      }
      const entry = this.log.find((item) => item.index === index);
      return entry ? entry.term : 0;
    }

    entryAt(index) {
      return this.log.find((item) => item.index === index) || null;
    }

    entriesAfter(index) {
      return this.log.filter((item) => item.index > index);
    }

    votesNeeded() {
      return majorityOf(this.peers.length + 1);
    }
  }

  // ---------- 生命周期 ----------
  Object.assign(RaftNode.prototype, {
    initTimers(now) {
      this.electionDeadline = now + randElectionTimeout(this.rng);
      this.peers.forEach((peer) => {
        this.lastContact[peer] = now;
      });
    },

    setDown(isDown, now) {
      this.down = isDown;
      if (isDown) {
        this.reason = '节点宕机（不处理任何 RPC）';
      } else {
        // 恢复后以 Follower 身份等待心跳，超时后再参与选举
        this.role = 'follower';
        this.leaderId = null;
        this.electionDeadline = now + randElectionTimeout(this.rng);
        this.reason = '节点恢复，等待 Leader 心跳';
        this.bus.emitNode(this.id, 'recover', '节点恢复，重新加入集群', now);
      }
    },

    becomeFollower(term, reason, now) {
      if (term > this.currentTerm) {
        this.currentTerm = term;
        this.votedFor = null;
      }
      if (this.role !== 'follower') {
        this.bus.emitNode(this.id, 'role', `降级为 Follower（term ${term}）：${reason}`, now);
      }
      this.role = 'follower';
      // 只有确认 Leader 存在时才重置选举时钟；单纯发现更高 term 不应立即重选，避免 term 抖动
      this.reason = reason;
    },

    resetElectionTimer(now) {
      this.electionDeadline = now + randElectionTimeout(this.rng);
    },

    becomeCandidate(now) {
      this.currentTerm += 1;
      this.role = 'candidate';
      this.votedFor = this.id;
      this.leaderId = null;
      this.electionStartTerm = this.currentTerm;
      this.votesGranted = { [this.id]: true };
      this.electionDeadline = now + randElectionTimeout(this.rng);
      this.reason = `选举超时，发起 term ${this.currentTerm} 选举`;
      this.bus.emitNode(this.id, 'role', `成为 Candidate，term ${this.currentTerm}，自投并拉票`, now);
      this.broadcastRequestVote(now);
    },

    becomeLeader(now) {
      this.role = 'leader';
      this.leaderId = this.id;
      const next = this.lastLogIndex + 1;
      this.peers.forEach((peer) => {
        this.nextIndex[peer] = next;
        this.matchIndex[peer] = 0;
        this.lastContact[peer] = now;
      });
      this.reason = `获得多数票，成为 term ${this.currentTerm} 的 Leader`;
      this.bus.emitNode(this.id, 'role', this.reason, now);
      // 提交一条 no-op，快速确立本 term 的提交能力（Raft 论文 8 建议）
      this.appendLocalEntry({ type: 'noop' });
      this.broadcastAppendEntries(now, true);
    },

    // ---------- tick ----------
    tick(now) {
      if (this.down) return;
      if (this.role === 'leader') this.leaderTick(now);
      else if (now >= this.electionDeadline) this.becomeCandidate(now);
      this.applyCommitted(now);
    },

    leaderTick(now) {
      // CheckQuorum：宽限期内未联系到多数派则主动降级，避免分区时出现双 Leader
      const alive = this.peers.filter(
        (peer) => now - this.lastContact[peer] <= RAFT_CONFIG.QUORUM_GRACE
      ).length + 1;
      if (alive < this.votesNeeded()) {
        this.becomeFollower(
          this.currentTerm,
          `分区/宕机导致 ${RAFT_CONFIG.QUORUM_GRACE}ms 内无法联系多数派，主动让出 Leader`,
          now
        );
        return;
      }
      if (now - this.lastHeartbeatSent >= RAFT_CONFIG.HEARTBEAT_INTERVAL) {
        this.broadcastAppendEntries(now, false);
      }
    },
  });

  // ---------- RequestVote ----------
  Object.assign(RaftNode.prototype, {
    broadcastRequestVote(now) {
      this.peers.forEach((peer) => {
        this.bus.send(this.id, peer, {
          type: 'RequestVote',
          term: this.currentTerm,
          candidateId: this.id,
          lastLogIndex: this.lastLogIndex,
          lastLogTerm: this.lastLogTerm,
        }, now);
      });
    },

    handleRequestVote(msg, now) {
      const req = msg.payload;
      let granted = false;
      let detail = '';

      if (req.term < this.currentTerm) {
        detail = `拒绝：候选 term ${req.term} 小于当前 term ${this.currentTerm}`;
      } else {
        if (req.term > this.currentTerm) {
          this.becomeFollower(req.term, '发现更高 term', now);
        }
        const logUpToDate =
          req.lastLogTerm > this.lastLogTerm ||
          (req.lastLogTerm === this.lastLogTerm &&
            req.lastLogIndex >= this.lastLogIndex);
        const canVote =
          this.votedFor === null ||
          this.votedFor === req.candidateId;

        if (canVote && logUpToDate) {
          granted = true;
          this.votedFor = req.candidateId;
          this.resetElectionTimer(now);
          this.reason = `投票给节点 ${req.candidateId}（term ${req.term}）`;
          detail = '同意：候选日志至少与本节点一样新';
        } else if (!logUpToDate) {
          detail = `拒绝：候选日志(${req.lastLogTerm}/${req.lastLogIndex}) 比本节点(${this.lastLogTerm}/${this.lastLogIndex})旧`;
        } else {
          detail = `拒绝：本 term 已投票给节点 ${this.votedFor}`;
        }
      }

      this.bus.reply(this.id, req.candidateId, {
        type: 'RequestVoteResp',
        term: this.currentTerm,
        voterId: this.id,
        granted,
        detail,
      }, now, { request: req, detail });
    },

    handleRequestVoteResponse(msg, now) {
      const resp = msg.payload;
      if (this.role !== 'candidate') return;
      if (resp.term > this.currentTerm) {
        this.becomeFollower(resp.term, `发现更高 term（来自节点 ${resp.voterId} 投票响应）`, now);
        return;
      }
      if (resp.term !== this.electionStartTerm) return;
      if (resp.granted) {
        this.votesGranted[resp.voterId] = true;
        if (Object.keys(this.votesGranted).length >= this.votesNeeded()) {
          this.becomeLeader(now);
        }
      }
    },
  });

  // ---------- AppendEntries ----------
  Object.assign(RaftNode.prototype, {
    appendLocalEntry(command) {
      const index = this.lastLogIndex + 1;
      const entry = makeEntry(this.currentTerm, command);
      const stored = Object.assign({ index, term: this.currentTerm }, entry);
      this.log.push(stored);
      this.matchIndex[this.id] = this.lastLogIndex;
      return stored;
    },

    clientCommand(command, now) {
      if (this.role !== 'leader') {
        return {
          ok: false,
          redirect: this.leaderId,
          error: this.leaderId == null
            ? '当前没有 Leader，请稍后重试'
            : `请转发给 Leader 节点 ${this.leaderId}`,
        };
      }
      const entry = this.appendLocalEntry(command);
      this.bus.emitNode(
        this.id,
        'client',
        `客户端命令追加为日志 #${entry.index}（term ${entry.term}），等待多数派提交`,
        now
      );
      this.broadcastAppendEntries(now, true);
      // 绝不能在此刻声称"提交成功"：只有多数派复制并推进 commitIndex 后才算确认。
      // 若本 Leader 处于少数派分区，该日志可能永远不会提交，并会在愈合后被新
      // Leader 覆盖截断 —— 因此返回 pending（挂起等待），由调用方跟踪终态。
      return {
        ok: true,
        pending: true,
        committed: false,
        index: entry.index,
        term: entry.term,
        uid: entry.uid,
        message: `已受理为日志 #${entry.index}，等待多数派提交确认`,
      };
    },

    entriesForPeer(peer) {
      const next = this.nextIndex[peer];
      return this.log.filter((item) => item.index >= next);
    },

    broadcastAppendEntries(now, immediate) {
      this.lastHeartbeatSent = now;
      this.peers.forEach((peer) => this.sendAppendOrSnapshot(peer, now));
    },

    sendAppendOrSnapshot(peer, now) {
      if (this.nextIndex[peer] == null) {
        this.nextIndex[peer] = this.lastLogIndex + 1;
        this.matchIndex[peer] = 0;
      }
      const next = this.nextIndex[peer];

      // Follower 需要的 prevLog 早于 Leader 最早可用日志（已压缩进快照）-> InstallSnapshot
      if (this.snapshot && next <= this.snapshot.lastIncludedIndex) {
        this.bus.send(this.id, peer, {
          type: 'InstallSnapshot',
          term: this.currentTerm,
          leaderId: this.id,
          lastIncludedIndex: this.snapshot.lastIncludedIndex,
          lastIncludedTerm: this.snapshot.lastIncludedTerm,
          data: clone(this.snapshot.data),
        }, now);
        return;
      }

      const prevLogIndex = next - 1;
      const prevLogTerm = this.termAt(prevLogIndex);
      const entries = clone(this.entriesForPeer(peer));
      this.bus.send(this.id, peer, {
        type: 'AppendEntries',
        term: this.currentTerm,
        leaderId: this.id,
        prevLogIndex,
        prevLogTerm,
        entries,
        leaderCommit: this.commitIndex,
        heartbeat: entries.length === 0,
      }, now);
    },

    handleAppendEntries(msg, now) {
      const req = msg.payload;
      let success = false;
      let conflictIndex = 0;
      let conflictTerm = 0;
      let detail = '';

      if (req.term < this.currentTerm) {
        detail = `拒绝：Leader term ${req.term} 过期（当前 term ${this.currentTerm}）`;
      } else {
        if (req.term > this.currentTerm || this.role === 'candidate') {
          this.becomeFollower(req.term, '收到合法 Leader 的 AppendEntries', now);
        }
        this.currentTerm = req.term;
        this.leaderId = req.leaderId;
        this.resetElectionTimer(now);

        // prevLog 已被快照覆盖：要求 Leader 发送快照，或快照点恰好兼容
        if (this.snapshot && req.prevLogIndex < this.snapshot.lastIncludedIndex) {
          if (
            req.prevLogIndex === 0 ||
            this.snapshot.lastIncludedIndex >= req.prevLogIndex
          ) {
            // 本节点快照比 Leader 的 prevLog 还新，让 Leader 按快照对齐
            conflictIndex = this.snapshot.lastIncludedIndex + 1;
            detail = `拒绝：本地已有快照 #${this.snapshot.lastIncludedIndex}，需要 Leader 对齐到快照之后`;
          }
        } else if (req.prevLogIndex > this.lastLogIndex) {
          conflictIndex = this.lastLogIndex + 1;
          detail = `拒绝：缺少日志 #${req.prevLogIndex}（本地最后 #${this.lastLogIndex}），快速回退`;
        } else if (this.termAt(req.prevLogIndex) !== req.prevLogTerm) {
          // 冲突截断优化：找到冲突 term 的第一条日志
          conflictTerm = this.termAt(req.prevLogIndex);
          let idx = req.prevLogIndex;
          while (idx > 0 && this.termAt(idx) === conflictTerm) idx -= 1;
          conflictIndex = idx + 1;
          detail = `拒绝：#${req.prevLogIndex} 的 term ${conflictTerm} 与 Leader 的 ${req.prevLogTerm} 冲突，截断到 #${conflictIndex}`;
        } else {
          success = true;
          detail = req.heartbeat
            ? '心跳正常'
            : `追加 ${req.entries.length} 条日志`;
          this.mergeEntries(req.entries);
          const newCommit = Math.min(req.leaderCommit, this.lastLogIndex);
          if (newCommit > this.commitIndex) {
            this.commitIndex = newCommit;
          }
        }
      }

      this.bus.reply(this.id, req.leaderId, {
        type: 'AppendEntriesResp',
        term: this.currentTerm,
        followerId: this.id,
        success,
        conflictIndex,
        conflictTerm,
        lastLogIndex: this.lastLogIndex,
        detail,
      }, now, { request: req, detail, success });
    },

    // Leader 强制覆盖：删除冲突后缀，写入 Leader 的条目
    mergeEntries(incoming) {
      incoming.forEach((entry) => {
        const existing = this.entryAt(entry.index);
        if (existing) {
          if (existing.term !== entry.term) {
            this.log = this.log.filter((item) => item.index < entry.index);
            this.bus.emitNode(
              this.id,
              'truncate',
              `日志冲突：截断 #${entry.index} 及之后的旧内容，强制覆盖为 Leader 版本`,
              null
            );
            this.log.push({ index: entry.index, term: entry.term, command: entry.command, uid: entry.uid });
          }
        } else {
          this.log.push({ index: entry.index, term: entry.term, command: entry.command, uid: entry.uid });
        }
      });
      this.log.sort((a, b) => a.index - b.index);
      this.matchIndex[this.id] = this.lastLogIndex;
    },

    handleAppendEntriesResponse(msg, now) {
      const resp = msg.payload;
      if (resp.term > this.currentTerm) {
        this.becomeFollower(resp.term, `发现更高 term（来自节点 ${resp.followerId} 响应）`, now);
        return;
      }
      if (this.role !== 'leader' || resp.term !== this.currentTerm) return;

      const peer = resp.followerId;
      this.lastContact[peer] = now;

      if (resp.success) {
        this.matchIndex[peer] = Math.max(this.matchIndex[peer] || 0, resp.lastLogIndex);
        this.nextIndex[peer] = (this.matchIndex[peer] || 0) + 1;
        this.advanceCommitIndex(now);
      } else {
        // 论文 5.3 的冲突提示快速回退
        if (resp.conflictIndex > 0) {
          let back = resp.conflictIndex;
          if (resp.conflictTerm > 0) {
            const leaderHas = this.log.some(
              (item) => item.term === resp.conflictTerm
            );
            if (leaderHas) {
              let idx = resp.conflictIndex;
              while (idx > 0 && this.termAt(idx) === resp.conflictTerm) idx -= 1;
              back = idx + 1;
            } else {
              back = resp.conflictIndex;
            }
          }
          this.nextIndex[peer] = Math.max(1, back);
        } else {
          this.nextIndex[peer] = Math.max(1, this.nextIndex[peer] - 1);
        }
        this.matchIndex[peer] = Math.min(
          this.matchIndex[peer] || 0,
          this.nextIndex[peer] - 1
        );
        this.sendAppendOrSnapshot(peer, now);
      }
    },

    advanceCommitIndex(now) {
      for (let n = this.lastLogIndex; n > this.commitIndex; n--) {
        if (this.termAt(n) !== this.currentTerm) continue; // 不按副本计数提交旧 term
        const replicated = this.peers.filter(
          (peer) => (this.matchIndex[peer] || 0) >= n
        ).length + 1;
        if (replicated >= this.votesNeeded()) {
          this.commitIndex = n;
          this.bus.emitNode(this.id, 'commit', `commitIndex 推进到 ${n}`, now);
          break;
        }
      }
    },
  });

  // ---------- Snapshot / InstallSnapshot ----------
  Object.assign(RaftNode.prototype, {
    handleInstallSnapshot(msg, now) {
      const req = msg.payload;
      let installed = false;
      let detail = '';

      if (req.term < this.currentTerm) {
        detail = `拒绝快照：term ${req.term} 过期`;
      } else {
        if (req.term > this.currentTerm || this.role === 'candidate') {
          this.becomeFollower(req.term, '收到 Leader 的 InstallSnapshot', now);
        }
        this.currentTerm = req.term;
        this.leaderId = req.leaderId;
        this.resetElectionTimer(now);

        const already =
          this.snapshot && this.snapshot.lastIncludedIndex >= req.lastIncludedIndex;
        if (already) {
          detail = `忽略旧快照（本地快照 #${this.snapshot.lastIncludedIndex} 更新）`;
        } else {
          this.snapshot = {
            lastIncludedIndex: req.lastIncludedIndex,
            lastIncludedTerm: req.lastIncludedTerm,
            data: clone(req.data),
          };
          // 丢弃快照点之前（含）的全部日志
          this.log = this.log.filter(
            (item) => item.index > req.lastIncludedIndex
          );

          this.state = clone(req.data);
          this.commitIndex = Math.max(this.commitIndex, req.lastIncludedIndex);
          this.lastApplied = Math.max(this.lastApplied, req.lastIncludedIndex);
          installed = true;
          detail = `安装快照到 #${req.lastIncludedIndex}，状态机重置为快照数据`;
          this.bus.emitNode(
            this.id,
            'snapshot',
            `接收 Leader 快照（截至 #${req.lastIncludedIndex}），压缩旧日志`,
            now
          );
        }
      }

      this.bus.reply(this.id, req.leaderId, {
        type: 'InstallSnapshotResp',
        term: this.currentTerm,
        followerId: this.id,
        lastIncludedIndex: req.lastIncludedIndex,
        installed,
        detail,
      }, now, { request: req, detail, success: installed });
    },

    handleInstallSnapshotResponse(msg, now) {
      const resp = msg.payload;
      if (resp.term > this.currentTerm) {
        this.becomeFollower(resp.term, '发现更高 term（快照响应）', now);
        return;
      }
      if (this.role !== 'leader') return;
      const peer = resp.followerId;
      this.lastContact[peer] = this.bus.now();
      if (resp.installed) {
        this.matchIndex[peer] = Math.max(
          this.matchIndex[peer] || 0,
          resp.lastIncludedIndex
        );
        this.nextIndex[peer] = Math.max(
          this.nextIndex[peer] || 1,
          resp.lastIncludedIndex + 1
        );
        this.advanceCommitIndex(this.bus.now());
        this.sendAppendOrSnapshot(peer, this.bus.now());
      }
    },

    // ---------- 状态机与压缩 ----------
    applyCommand(command) {
      if (!command || command.type === 'noop') return;
      if (command.type === 'set') {
        this.state[command.key] = command.value;
      } else if (command.type === 'delete') {
        delete this.state[command.key];
      }
    },

    applyCommitted(now) {
      while (this.lastApplied < this.commitIndex) {
        const next = this.lastApplied + 1;
        const entry = this.entryAt(next);
        if (!entry) break; // 被快照覆盖或尚未到达
        this.applyCommand(entry.command);
        this.lastApplied = next;
        this.bus.emitNode(
          this.id,
          'apply',
          `应用日志 #${next} 到状态机`,
          now
        );
        // 所有角色都按统一阈值自动压缩，保证各节点快照点一致、界面可比；
        // 极度落后的节点不在这里压缩，而是通过 Leader 的 InstallSnapshot 追平。
        if (
          this.bus.autoSnapshot &&
          this.log.length >= RAFT_CONFIG.SNAPSHOT_THRESHOLD
        ) {
          const cut = this.log[0].index + RAFT_CONFIG.SNAPSHOT_THRESHOLD - 1;
          if (cut <= this.lastApplied) this.compact(cut, now, true);
        }
      }
    },

    compact(index, now, automatic) {
      if (this.down) return { ok: false, error: '节点宕机中' };
      if (index <= 0 || index > this.lastApplied) {
        return { ok: false, error: '只能压缩已应用（lastApplied）的日志' };
      }
      // 非 Leader 的手动压缩对齐到统一阈值快照点，保证各节点快照点一致、
      // 且仍能利用普通 AppendEntries 增量同步（不会制造 InstallSnapshot 需求）。
      if (!automatic && this.role !== 'leader' && this.log.length) {
        const aligned = this.log[0].index + RAFT_CONFIG.SNAPSHOT_THRESHOLD - 1;
        if (aligned < index) index = aligned;
      }
      const cut = this.entryAt(index);
      if (!cut) {
        return { ok: false, error: `#${index} 已被压缩或不存在` };
      }
      this.snapshot = {
        lastIncludedIndex: index,
        lastIncludedTerm: cut.term,
        data: clone(this.state),
      };
      this.log = this.log.filter((item) => item.index > index);
      this.bus.emitNode(
        this.id,
        'snapshot',
        `${automatic ? '自动' : '手动'}创建快照（截至 #${index}，term ${cut.term}），丢弃压缩日志`,
        now
      );
      if (this.role === 'leader') {
        this.peers.forEach((peer) => {
          if ((this.nextIndex[peer] || 1) <= index) {
            this.nextIndex[peer] = index + 1; // InstallSnapshot 成功后从快照点之后继续
          }
        });
      }
      return { ok: true, index };
    },

    // ---------- 消息入口 ----------
    receive(message, now) {
      if (this.down) return;
      switch (message.type) {
        case 'RequestVote':
          this.handleRequestVote(message, now);
          break;
        case 'RequestVoteResp':
          this.handleRequestVoteResponse(message, now);
          break;
        case 'AppendEntries':
          this.handleAppendEntries(message, now);
          break;
        case 'AppendEntriesResp':
          this.handleAppendEntriesResponse(message, now);
          break;
        case 'InstallSnapshot':
          this.handleInstallSnapshot(message, now);
          break;
        case 'InstallSnapshotResp':
          this.handleInstallSnapshotResponse(message, now);
          break;
        default:
          break;
      }
    },
  });

  global.RaftNode = RaftNode;
  global.__makeEntry = makeEntry;
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = { RaftNode, makeEntry, setEntrySeq: (v) => { ENTRY_SEQ = v; } };
  }
})(typeof window !== 'undefined' ? window : globalThis);
