// 全局配置与工具函数：所有时间单位均为模拟毫秒，由 Sim 统一推进。
(function (global) {
  const RAFT_CONFIG = {
    TICK_MS: 20,
    HEARTBEAT_INTERVAL: 650,
    ELECTION_TIMEOUT_MIN: 2300,
    ELECTION_TIMEOUT_SPREAD: 1400,
    QUORUM_GRACE: 2000,
    SNAPSHOT_THRESHOLD: 5,
    MAX_RPC_LOG: 140,
  };

  function majorityOf(n) {
    return Math.floor(n / 2) + 1;
  }

  function randElectionTimeout(rng) {
    return (
      RAFT_CONFIG.ELECTION_TIMEOUT_MIN +
        Math.floor(rng() * RAFT_CONFIG.ELECTION_TIMEOUT_SPREAD)
    );
  }

  function clone(value) {
    return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
  }

  function esc(text) {
    return String(text === undefined || text === null ? '' : text).replace(
      /[&<>"]/g,
      (ch) =>
        ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[ch])
    );
  }

  const api = { RAFT_CONFIG, majorityOf, randElectionTimeout, clone, esc };
  global.RAFT_CONFIG = RAFT_CONFIG;
  global.RAFT_COMMON = api;
  if (typeof module !== 'undefined' && module.exports) module.exports = global;
})(typeof window !== 'undefined' ? window : globalThis);
