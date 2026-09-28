// 模拟器：维护仿真时钟、节点集合、网络、事件日志，并驱动每个 tick。
(function (global) {
  const { RAFT_CONFIG } = global.RAFT_COMMON;
  const { RaftNode } = global;

  function mulberry32(seed) {
    return function () {
      let t = (seed += 0x6d2b79f5);
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }

  class Sim {
    constructor(options) {
      options = options || {};
      this.seed = options.seed || 42;
      this.rng = mulberry32(this.seed);
      this.time = 0;
      this.running = false;
      this.speed = options.speed != null ? options.speed : 1;
      this.autoSnapshot = options.autoSnapshot !== false;
      this.events = [];
      this.rpcs = [];
      this.seq = 0;
      this.pendingCommands = [];
      this.changeListeners = [];
      this.network = new global.Network([], this.rng);
      this.nodes = {};
      this.nodeOrder = [];
      this.onCommit = options.onCommit || function () {};
      this.setNodeCount(options.nodeCount || 3);
    }

    onChange(fn) {
      this.changeListeners.push(fn);
    }

    emitChange() {
      this.changeListeners.forEach((fn) => fn(this));
    }

    nextSeq() {
      this.seq += 1;
      return this.seq;
    }

    now() {
      return this.time;
    }

    setNodeCount(count) {
      const ids = [];
      for (let i = 1; i <= count; i++) ids.push(i);
      this.network.reset(ids);
      this.nodes = {};
      this.nodeOrder = ids;
      this.time = 0;
      this.events = [];
      this.rpcs = [];
      this.seq = 0;
      this.pendingCommands = [];
      const bus = this.makeBus();
      ids.forEach((id) => {
        const peers = ids.filter((other) => other !== id);
        this.nodes[id] = new RaftNode(id, peers, bus, this.rng);
      });
      ids.forEach((id) => this.nodes[id].initTimers(0));
      this.events.push({
        seq: this.nextSeq(),
        time: 0,
        kind: 'system',
        nodeId: null,
        text: `初始化 ${count} 节点集群（随机种子 ${this.seed}）`,
      });
    }

    makeBus() {
      const sim = this;
      return {
        now: () => sim.time,
        autoSnapshot: sim.autoSnapshot,
        dispatch(from, to, payload, now, status, detail, success) {
          const message = {
            id: 'm' + sim.nextSeq(),
            from,
            to,
            type: payload.type,
            payload,
            sentAt: now,
          };
          const queued = sim.network.send(message, now, () => {});
          sim.recordRpc({
            direction: from + '→' + to,
            type: payload.type,
            term: payload.term,
            time: now,
            status: queued ? status : 'dropped',
            detail: queued
              ? detail || (payload.heartbeat ? '心跳' : '')
              : '不可达：对端宕机或处于另一分区',
            success: queued ? success : false,
          });
        },
        send(from, to, payload, now) {
          this.dispatch(from, to, payload, now, 'sent');
        },
        reply(from, to, payload, now, meta) {
          this.dispatch(
            from,
            to,
            payload,
            now,
            'reply',
            (meta && meta.detail) || '',
            meta ? meta.success : undefined
          );
        },
        emitNode(nodeId, kind, text, at) {
          sim.events.push({
            seq: sim.nextSeq(),
            time: at == null ? sim.time : at,
            kind,
            nodeId,
            text,
          });
          if (sim.events.length > 400) sim.events.splice(0, sim.events.length - 400);
        },
      };
    }

    recordRpc(entry) {
      this.rpcs.push(Object.assign({ seq: this.nextSeq() }, entry));
      if (this.rpcs.length > RAFT_CONFIG.MAX_RPC_LOG) {
        this.rpcs.splice(0, this.rpcs.length - RAFT_CONFIG.MAX_RPC_LOG);
      }
    }


    setDown(id, isDown) {
      this.network.setDown(id, isDown);
      this.nodes[id].setDown(isDown, this.time);
      this.events.push({
        seq: this.nextSeq(),
        time: this.time,
        kind: 'system',
        nodeId: id,
        text: isDown ? '手动宕机' : '手动恢复',
      });
    }

    setLatency(id, ms) {
      this.network.setLatency(id, ms);
      this.events.push({
        seq: this.nextSeq(),
        time: this.time,
        kind: 'system',
        nodeId: id,
        text: `手动设置链路延迟 ${ms}ms（双向）`,
      });
    }

    partitionMinority() {
      // 通用少数派分区：把最后一个节点单独隔离
      const victim = this.nodeOrder[this.nodeOrder.length - 1];
      this.network.isolate(victim);
      this.events.push({
        seq: this.nextSeq(),
        time: this.time,
        kind: 'system',
        nodeId: null,
        text: `网络分区：节点 ${victim} 被隔离，其余节点保持多数派`,
      });
    }

    partitionCustom(groups) {
      this.network.assignGroups(groups);
      this.events.push({
        seq: this.nextSeq(),
        time: this.time,
        kind: 'system',
        nodeId: null,
        text:
          '自定义分区：' +
          groups.map((g) => '[' + g.join(',') + ']').join(' / '),
      });
    }

    heal() {
      this.network.heal();
      this.events.push({
        seq: this.nextSeq(),
        time: this.time,
        kind: 'system',
        nodeId: null,
        text: '网络恢复：所有分区重新连通',
      });
    }

    isolate(id) {
      this.network.isolate(id);
      this.events.push({
        seq: this.nextSeq(),
        time: this.time,
        kind: 'system',
        nodeId: id,
        text: '单独隔离该节点',
      });
    }

    leader() {
      return this.nodeOrder
        .map((id) => this.nodes[id])
        .find((node) => node.role === 'leader' && !node.down) || null;
    }

    submitCommand(command) {
      let target = this.leader();
      if (!target) {
        // UI 兜底：任意在线节点都会返回重定向信息
        target = this.nodeOrder.map((id) => this.nodes[id]).find((n) => !n.down);
      }
      if (!target) return { ok: false, error: '所有节点均宕机' };
      const result = target.clientCommand(command, this.time);
      if (result.ok && result.pending) {
        // 登记挂起命令：只有多数派提交才算确认；被新 Leader 覆盖则明确判负
        this.pendingCommands.push({
          uid: result.uid,
          nodeId: target.id,
          index: result.index,
          term: result.term,
          command,
          status: 'pending',
          reason: null,
          submittedAt: this.time,
        });
      }
      if (!result.ok && result.redirect == null) {
        this.events.push({
          seq: this.nextSeq(),
          time: this.time,
          kind: 'client-fail',
          nodeId: target.id,
          text: result.error,
        });
      }
      return result;
    }

    commandStatus(uid) {
      if (uid == null) return null;
      return this.pendingCommands.find((rec) => rec.uid === uid) || null;
    }

    // 每个 tick 结算挂起中的客户端命令：
    // - 任一节点 commitIndex 覆盖该日志且 term 相符 -> committed（真正确认）
    // - 原 Leader 已下台且本地该位置被新 Leader 覆盖 -> lost（明确失败，可安全重试）
    resolvePendingCommands() {
      this.pendingCommands.forEach((rec) => {
        if (rec.status !== 'pending') return;
        const committed = this.nodeOrder.some((id) => {
          const node = this.nodes[id];
          return (
            node.commitIndex >= rec.index && node.termAt(rec.index) === rec.term
          );
        });
        if (committed) {
          rec.status = 'committed';
          rec.reason = '已获多数派复制并提交';
          this.events.push({
            seq: this.nextSeq(),
            time: this.time,
            kind: 'client-commit',
            nodeId: rec.nodeId,
            text: `客户端命令 #${rec.index} 获多数派提交，确认成功`,
          });
          this.onCommit(rec);
          return;
        }
        const origin = this.nodes[rec.nodeId];
        const overwritten =
          origin.role !== 'leader' && origin.termAt(rec.index) !== rec.term;
        if (overwritten) {
          rec.status = 'lost';
          rec.reason = `日志 #${rec.index} 未获多数派提交，已被新 Leader 覆盖截断`;
          this.events.push({
            seq: this.nextSeq(),
            time: this.time,
            kind: 'client-lost',
            nodeId: rec.nodeId,
            text:
              `客户端命令 #${rec.index} 未提交即被新 Leader 覆盖，` +
              '明确失败（客户端从未收到成功确认，可安全重试）',
          });
        }
      });
    }

    compactNode(id) {
      return this.nodes[id].compact(this.nodes[id].lastApplied, this.time, false);
    }

    clearRpcs() {
      this.rpcs = [];
    }

    step(realElapsedMs) {
      // realElapsedMs 为真实流逝毫秒；speed 为仿真加速倍率
      const simElapsed = (realElapsedMs || RAFT_CONFIG.TICK_MS) * this.speed;
      const until = this.time + simElapsed;
      while (this.time < until) {
        this.time += RAFT_CONFIG.TICK_MS;
        this.nodeOrder.forEach((id) => this.nodes[id].tick(this.time));
        this.network.deliverDue(
          this.time,
          this.mapById((node) => (message, now) => {
            node.receive(
              Object.assign({}, message, { deliveredAt: now }),
              now
            );
          }),
          (message, now) => {
            // 实际成功投递（发送时记录的是“已发出”）
          }
        );
      }
      this.resolvePendingCommands();
      this.emitChange();
    }

    mapById(fn) {
      const map = {};
      this.nodeOrder.forEach((id) => {
        map[id] = fn(this.nodes[id]);
      });
      return map;
    }

    start() {
      if (this._timer) return;
      this.running = true;
      let last = Date.now();
      this._timer = setInterval(() => {
        const realNow = Date.now();
        const elapsed = Math.min(100, realNow - last);
        last = realNow;
        this.step(elapsed);
      }, RAFT_CONFIG.TICK_MS);
    }

    pause() {
      this.running = false;
      if (this._timer) {
        clearInterval(this._timer);
        this._timer = null;
      }
    }
  }

  global.Sim = Sim;
  if (typeof module !== 'undefined' && module.exports) module.exports = { Sim };
})(typeof window !== 'undefined' ? window : globalThis);

