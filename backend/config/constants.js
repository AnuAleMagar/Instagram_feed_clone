/**
 * Application Constants
 * Centralized configuration for magic numbers and system limits
 */

// Feed Configuration
export const FEED_CONFIG = {
  MAX_FEED_SIZE: 100, // Maximum posts to keep in Redis feed cache
  FEED_TTL: 7 * 24 * 60 * 60, // 7 days in seconds
  POST_CACHE_TTL: 60 * 60, // 1 hour in seconds
  RESPONSE_CACHE_TTL: 7 * 24 * 60 * 60, // 7 days in seconds
  MIN_CACHED_POSTS_RATIO: 0.8, // 80% of requested limit must be cached
  BACKFILL_BATCH_SIZE: 1000, // Process backfill in batches
  FANOUT_BATCH_SIZE: 500, // Process fan-out in batches to prevent OOM
};

// Kafka Configuration
export const KAFKA_CONFIG = {
  MAX_RETRIES: 5, // Maximum retry attempts for failed messages
  INITIAL_RETRY_DELAY: 1000, // Initial retry delay in ms
  MAX_RETRY_DELAY: 60000, // Maximum retry delay in ms (1 minute)
  RETRY_MULTIPLIER: 2, // Exponential backoff multiplier
  DLQ_TOPIC_SUFFIX: "-dlq", // Dead-letter queue topic suffix
};

// Database Configuration
export const DB_CONFIG = {
  POSTGRES: {
    POOL_MAX: parseInt(process.env.DB_POOL_MAX || "20"),
    POOL_MIN: parseInt(process.env.DB_POOL_MIN || "5"),
    POOL_ACQUIRE: 30000,
    POOL_IDLE: 10000,
  },
  REDIS: {
    MAX_RETRIES: 3,
    RETRY_DELAY: 1000,
  },
};

// Rate Limiting Configuration
export const RATE_LIMIT_CONFIG = {
  WINDOW_MS: 60 * 1000, // 1 minute instead of 15 minutes
  MAX_REQUESTS: 1000, // Much higher limit
  POST_CREATE_LIMIT: 1000, // Much higher limit
  FEED_FETCH_LIMIT: 1000, // Much higher limit
};

// Error Handling Configuration
export const ERROR_CONFIG = {
  MAX_ERROR_LOG_LENGTH: 500, // Truncate error messages to this length
  RETRYABLE_ERRORS: [
    "ECONNRESET",
    "ETIMEDOUT",
    "ENOTFOUND",
    "ECONNREFUSED",
    "EAI_AGAIN",
  ],
};
