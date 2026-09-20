// 网络模型：负责节点之间的可达性（宕机 / 分区）、传播延迟与消息队列。
(function (global) {
  const { clone } = global.RAFT_COMMON;

  class Network {
    constructor(nodeIds, rng) {
      this.rng = rng || Math.random;
      this.setNodes(nodeIds);
    }

    setNodes(nodeIds) {
      this.nodeIds = nodeIds.slice();
      this.down = new Set();
      this.groups = new Map();
      nodeIds.forEach((id) => this.groups.set(id, 0));
      this.latency = {};
      nodeIds.forEach((a) => {
        this.latency[a] = {};
        nodeIds.forEach((b) => {
          this.latency[a][b] = 25;
        });
      });
      this.baseLatency = 25;
      this.queue = [];
    }

    reset(nodeIds) {
      this.setNodes(nodeIds);
    }

    setDown(id, isDown) {
      if (isDown) this.down.add(id);
      else this.down.delete(id);
    }

    isDown(id) {
      return this.down.has(id);
    }

    setLatency(id, ms) {
      const value = Math.max(0, Math.min(5000, Number(ms) || 0));
      this.nodeIds.forEach((other) => {
        if (other === id) return;
        this.latency[id][other] = value;
        this.latency[other][id] = value;
      });
    }

    getLatency(id) {
      const values = this.nodeIds
        .filter((other) => other !== id)
        .map((other) => this.latency[id][other]);
      return values.length ? Math.round(values.reduce((a, b) => a + b, 0) / values.length) : 0;
    }

    assignGroups(groups) {
      // groups: [[a,b],[c]]，未列出的节点保留原组
      groups.forEach((group, index) => {
        group.forEach((id) => this.groups.set(id, index + 1));
      });
    }

    heal() {
      this.nodeIds.forEach((id) => this.groups.set(id, 0));
    }

    isolate(id) {
      // 分配一个全集群唯一的组，等价于与所有人隔离
      const unique = 1000 + id;
      this.groups.set(id, unique);
    }

    canDeliver(from, to) {
      if (from === to) return true;
      if (this.down.has(from) || this.down.has(to)) return false;
      if (this.groups.get(from) !== this.groups.get(to)) return false;
      return true;
    }

    send(message, now, onDrop) {
      const { from, to } = message;
      if (!this.canDeliver(from, to)) {
        onDrop(clone(message), now);
        return false;
      }
      const jitter = Math.floor(this.rng() * 20);
      const delay = (this.latency[from] && this.latency[from][to] != null
        ? this.latency[from][to]
        : this.baseLatency) + jitter;
      this.queue.push({ deliverAt: now + Math.max(1, delay), message: clone(message) });
      return true;
    }

    // 模拟“发送瞬间”因对端宕机 / 分区而立即丢失
    sendWouldDrop(from, to) {
      return !this.canDeliver(from, to);
    }

    deliverDue(now, handlers, onDeliver) {
      const due = this.queue.filter((item) => item.deliverAt <= now);
      this.queue = this.queue.filter((item) => item.deliverAt > now);
      // 按时间、再按插入顺序投递，保证同一时刻行为确定
      due.sort((a, b) => a.deliverAt - b.deliverAt);
      due.forEach((item) => {
        const { message } = item;
        if (this.down.has(message.to)) {
          return;
        }
        if (!this.canDeliver(message.from, message.to)) {
          return;
        }
        const handler = handlers[message.to];
        if (handler) handler(message, now);
        onDeliver(message, now);
      });
    }

    pendingCount(to) {
      return this.queue.filter((item) => item.message.to === to).length;
    }
  }

  global.Network = Network;
  if (typeof module !== 'undefined' && module.exports) module.exports = { Network };
})(typeof window !== 'undefined' ? window : globalThis);
