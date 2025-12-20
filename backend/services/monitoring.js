/**
 * Basic Monitoring and Metrics Service
 * Tracks key performance indicators and system health
 */

import { redisClient } from "../config/db.js";

// In-memory metrics (could be moved to Redis for persistence)
const metrics = {
  requests: {
    total: 0,
    errors: 0,
    byEndpoint: {},
  },
  kafka: {
    messagesProcessed: 0,
    messagesFailed: 0,
    dlqMessages: 0,
  },
  cache: {
    hits: 0,
    misses: 0,
  },
  database: {
    queries: 0,
    errors: 0,
  },
  startTime: Date.now(),
};

/**
 * Increment request counter
 * @param {string} endpoint - API endpoint
 * @param {boolean} isError - Whether request resulted in error
 */
export function recordRequest(endpoint, isError = false) {
  metrics.requests.total++;
  if (isError) {
    metrics.requests.errors++;
  }

  if (!metrics.requests.byEndpoint[endpoint]) {
    metrics.requests.byEndpoint[endpoint] = { total: 0, errors: 0 };
  }
  metrics.requests.byEndpoint[endpoint].total++;
  if (isError) {
    metrics.requests.byEndpoint[endpoint].errors++;
  }
}

/**
 * Record Kafka message processing
 * @param {boolean} success - Whether processing succeeded
 * @param {boolean} isDLQ - Whether message went to DLQ
 */
export function recordKafkaMessage(success, isDLQ = false) {
  if (success) {
    metrics.kafka.messagesProcessed++;
  } else {
    metrics.kafka.messagesFailed++;
    if (isDLQ) {
      metrics.kafka.dlqMessages++;
    }
  }
}

/**
 * Record cache operation
 * @param {boolean} isHit - Whether cache hit
 */
export function recordCacheOperation(isHit) {
  if (isHit) {
    metrics.cache.hits++;
  } else {
    metrics.cache.misses++;
  }
}

/**
 * Record database operation
 * @param {boolean} isError - Whether query resulted in error
 */
export function recordDatabaseOperation(isError = false) {
  metrics.database.queries++;
  if (isError) {
    metrics.database.errors++;
  }
}

/**
 * Get current metrics
 * @returns {Object} Current metrics snapshot
 */
export function getMetrics() {
  const uptime = Date.now() - metrics.startTime;
  const cacheHitRate =
    metrics.cache.hits + metrics.cache.misses > 0
      ? (metrics.cache.hits / (metrics.cache.hits + metrics.cache.misses)) * 100
      : 0;

  return {
    ...metrics,
    uptime: {
      ms: uptime,
      seconds: Math.floor(uptime / 1000),
      minutes: Math.floor(uptime / 60000),
      hours: Math.floor(uptime / 3600000),
    },
    cache: {
      ...metrics.cache,
      hitRate: `${cacheHitRate.toFixed(2)}%`,
    },
    errorRate:
      metrics.requests.total > 0
        ? `${((metrics.requests.errors / metrics.requests.total) * 100).toFixed(
            2
          )}%`
        : "0%",
  };
}

/**
 * Get Redis memory info
 * @returns {Promise<Object>} Redis memory information
 */
export async function getRedisMemoryInfo() {
  try {
    const info = await redisClient.info("memory");
    const lines = info.split("\n");
    const memoryInfo = {};

    lines.forEach((line) => {
      if (line.includes(":")) {
        const [key, value] = line.split(":");
        memoryInfo[key.trim()] = value.trim();
      }
    });

    return {
      usedMemory: memoryInfo.used_memory,
      usedMemoryHuman: memoryInfo.used_memory_human,
      maxMemory: memoryInfo.maxmemory,
      maxMemoryHuman: memoryInfo.maxmemory_human || "not set",
      evictedKeys: memoryInfo.evicted_keys || "0",
    };
  } catch (error) {
    console.error("Error getting Redis memory info:", error.message);
    return { error: error.message };
  }
}

/**
 * Reset metrics (for testing or periodic reset)
 */
export function resetMetrics() {
  metrics.requests.total = 0;
  metrics.requests.errors = 0;
  metrics.requests.byEndpoint = {};
  metrics.kafka.messagesProcessed = 0;
  metrics.kafka.messagesFailed = 0;
  metrics.kafka.dlqMessages = 0;
  metrics.cache.hits = 0;
  metrics.cache.misses = 0;
  metrics.database.queries = 0;
  metrics.database.errors = 0;
}
