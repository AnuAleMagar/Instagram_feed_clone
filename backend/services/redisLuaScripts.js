import { redisClient } from "../config/db.js";

/**
 * LUA Scripts for optimized Redis operations
 * These scripts execute atomically on the Redis server, reducing network round-trips
 */

// Script 1: Get count with TTL refresh (atomic GET + EXPIRE)
// Returns: count value or nil
const GET_COUNT_WITH_TTL_SCRIPT = `
  local value = redis.call('GET', KEYS[1])
  if value then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
    return value
  end
  return nil
`;

// Script 2: Increment count with TTL refresh
// Returns: new count value
const INCREMENT_COUNT_WITH_TTL_SCRIPT = `
  local newValue = redis.call('INCR', KEYS[1])
  redis.call('EXPIRE', KEYS[1], ARGV[1])
  return newValue
`;

// Script 3: Decrement count with TTL refresh and bounds checking (don't go below 0)
// Returns: new count value
const DECREMENT_COUNT_WITH_TTL_SCRIPT = `
  local newValue = redis.call('DECR', KEYS[1])
  if newValue < 0 then
    redis.call('SET', KEYS[1], '0')
    newValue = 0
  end
  redis.call('EXPIRE', KEYS[1], ARGV[1])
  return newValue
`;

// Script 4: Add post to feed with trim and TTL (atomic ZADD + ZREMRANGEBYRANK + EXPIRE)
// KEYS[1] = feed key
// ARGV[1] = score (timestamp)
// ARGV[2] = postId
// ARGV[3] = maxFeedSize
// ARGV[4] = TTL
// Returns: number of elements added (1 or 0)
const ADD_POST_TO_FEED_SCRIPT = `
  local added = redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
  local size = redis.call('ZCARD', KEYS[1])
  local maxSize = tonumber(ARGV[3])
  if size > maxSize then
    local removeCount = size - maxSize
    redis.call('ZREMRANGEBYRANK', KEYS[1], 0, removeCount - 1)
  end
  redis.call('EXPIRE', KEYS[1], ARGV[4])
  return added
`;

// Script 5: Get feed with TTL refresh (atomic ZRANGE + EXPIRE)
// KEYS[1] = feed key
// ARGV[1] = start index
// ARGV[2] = end index
// ARGV[3] = TTL
// Returns: array of [value1, score1, value2, score2, ...]
const GET_FEED_WITH_TTL_SCRIPT = `
  local result = redis.call('ZREVRANGE', KEYS[1], ARGV[1], ARGV[2], 'WITHSCORES')
  if #result > 0 then
    redis.call('EXPIRE', KEYS[1], ARGV[3])
  end
  return result
`;

// Script 6: Remove multiple posts from feed atomically
// KEYS[1] = feed key
// ARGV[1...N] = postIds to remove
// Returns: number of elements removed
const REMOVE_POSTS_FROM_FEED_SCRIPT = `
  local removed = 0
  for i = 1, #ARGV do
    removed = removed + redis.call('ZREM', KEYS[1], ARGV[i])
  end
  return removed
`;

// Script 7: Get feed with post details in one atomic operation (optimized)
// KEYS[1] = feed key
// ARGV[1] = start index
// ARGV[2] = end index
// ARGV[3] = TTL
// Returns: array where [1] = feed result, [2] = posts result
// Feed result: [postId1, score1, postId2, score2, ...]
// Posts result: [post1_json, post2_json, ...] or nil if not cached
const GET_FEED_WITH_POSTS_SCRIPT = `
  -- Get feed post IDs with scores
  local feedResult = redis.call('ZREVRANGE', KEYS[1], ARGV[1], ARGV[2], 'WITHSCORES')
  
  -- Refresh TTL if feed exists
  if #feedResult > 0 then
    redis.call('EXPIRE', KEYS[1], ARGV[3])
  end
  
  -- Extract post IDs from feed result (every other element starting from 1)
  local postIds = {}
  for i = 1, #feedResult, 2 do
    table.insert(postIds, feedResult[i])
  end
  
  -- Build post keys
  local postKeys = {}
  for i = 1, #postIds do
    table.insert(postKeys, 'post:' .. postIds[i])
  end
  
  -- Get post details using MGET
  local postsResult = {}
  if #postKeys > 0 then
    postsResult = redis.call('MGET', unpack(postKeys))
  end
  
  -- Return: feed result and posts result
  return {feedResult, postsResult}
`;

// Script 8: Warm up cache with multiple posts (atomic batch operation)
// KEYS[1] = feed key
// ARGV[1] = TTL
// ARGV[2] = maxFeedSize
// ARGV[3...N] = alternating score and postId pairs (score1, postId1, score2, postId2, ...)
// Returns: number of elements added
const WARM_UP_FEED_CACHE_SCRIPT = `
  local added = 0
  local ttl = tonumber(ARGV[1])
  local maxSize = tonumber(ARGV[2])
  
  -- Add all posts (ARGV[3] onwards are score, postId pairs)
  for i = 3, #ARGV, 2 do
    if ARGV[i + 1] then
      added = added + redis.call('ZADD', KEYS[1], ARGV[i], ARGV[i + 1])
    end
  end
  
  -- Trim if necessary
  local size = redis.call('ZCARD', KEYS[1])
  if size > maxSize then
    local removeCount = size - maxSize
    redis.call('ZREMRANGEBYRANK', KEYS[1], 0, removeCount - 1)
  end
  
  -- Set TTL
  redis.call('EXPIRE', KEYS[1], ttl)
  
  return added
`;

// Script 9: Get cached feed response (pre-serialized JSON)
// KEYS[1] = feed key
// ARGV[1] = TTL
// Returns: cached JSON string or nil
const GET_CACHED_FEED_RESPONSE_SCRIPT = `
  local feedKey = KEYS[1]
  local cachedResponseKey = feedKey .. ':response'
  
  -- Get cached response
  local cached = redis.call('GET', cachedResponseKey)
  
  if cached then
    -- Refresh TTL on both keys
    redis.call('EXPIRE', feedKey, ARGV[1])
    redis.call('EXPIRE', cachedResponseKey, ARGV[1])
    return cached
  end
  
  return nil
`;

// Script 10: Batch invalidate response caches for multiple users (atomic operation)
// ARGV[1...N] = user IDs
// Deletes feed:user:{userId}:response for each user
// Note: We only invalidate response cache, not feed cache (which is already updated)
// Returns: number of keys deleted
const BATCH_INVALIDATE_RESPONSE_CACHES_SCRIPT = `
  local deleted = 0
  for i = 1, #ARGV do
    local userId = ARGV[i]
    local responseKey = 'feed:user:' .. userId .. ':response'
    
    -- Delete response cache key
    if redis.call('DEL', responseKey) == 1 then
      deleted = deleted + 1
    end
  end
  return deleted
`;

// Load scripts and cache SHA1 hashes for performance
let scriptHashes = {};

/**
 * Initialize and load all LUA scripts
 * This should be called once when the application starts
 */
export const loadLuaScripts = async () => {
  try {
    scriptHashes = {
      getCountWithTTL: await redisClient.scriptLoad(GET_COUNT_WITH_TTL_SCRIPT),
      incrementCountWithTTL: await redisClient.scriptLoad(
        INCREMENT_COUNT_WITH_TTL_SCRIPT
      ),
      decrementCountWithTTL: await redisClient.scriptLoad(
        DECREMENT_COUNT_WITH_TTL_SCRIPT
      ),
      addPostToFeed: await redisClient.scriptLoad(ADD_POST_TO_FEED_SCRIPT),
      getFeedWithTTL: await redisClient.scriptLoad(GET_FEED_WITH_TTL_SCRIPT),
      removePostsFromFeed: await redisClient.scriptLoad(
        REMOVE_POSTS_FROM_FEED_SCRIPT
      ),
      warmUpFeedCache: await redisClient.scriptLoad(WARM_UP_FEED_CACHE_SCRIPT),
      getFeedWithPosts: await redisClient.scriptLoad(
        GET_FEED_WITH_POSTS_SCRIPT
      ),
      getCachedFeedResponse: await redisClient.scriptLoad(
        GET_CACHED_FEED_RESPONSE_SCRIPT
      ),
      batchInvalidateResponseCaches: await redisClient.scriptLoad(
        BATCH_INVALIDATE_RESPONSE_CACHES_SCRIPT
      ),
    };
    // LUA scripts loaded
    return scriptHashes;
  } catch (error) {
    console.error("❌ Error loading LUA scripts:", error);
    throw error;
  }
};

/**
 * Get the script source by name
 */
const getScriptSource = (scriptName) => {
  const scripts = {
    getCountWithTTL: GET_COUNT_WITH_TTL_SCRIPT,
    incrementCountWithTTL: INCREMENT_COUNT_WITH_TTL_SCRIPT,
    decrementCountWithTTL: DECREMENT_COUNT_WITH_TTL_SCRIPT,
    addPostToFeed: ADD_POST_TO_FEED_SCRIPT,
    getFeedWithTTL: GET_FEED_WITH_TTL_SCRIPT,
    removePostsFromFeed: REMOVE_POSTS_FROM_FEED_SCRIPT,
    warmUpFeedCache: WARM_UP_FEED_CACHE_SCRIPT,
    getFeedWithPosts: GET_FEED_WITH_POSTS_SCRIPT,
    getCachedFeedResponse: GET_CACHED_FEED_RESPONSE_SCRIPT,
    batchInvalidateResponseCaches: BATCH_INVALIDATE_RESPONSE_CACHES_SCRIPT,
  };
  return scripts[scriptName];
};

/**
 * Execute LUA script by SHA1 hash (faster than sending script each time)
 * Falls back to EVAL if script not cached
 */
const executeScript = async (scriptName, keys, args) => {
  try {
    const hash = scriptHashes[scriptName];
    if (hash) {
      // Try evalSha first (faster)
      try {
        return await redisClient.evalSha(hash, {
          keys,
          arguments: args,
        });
      } catch (evalError) {
        // If NOSCRIPT error, script was flushed, reload and use EVAL
        if (evalError.message && evalError.message.includes("NOSCRIPT")) {
          console.warn(
            `⚠️ Script ${scriptName} not found in Redis, using EVAL...`
          );
          const scriptSource = getScriptSource(scriptName);
          return await redisClient.eval(scriptSource, {
            keys,
            arguments: args,
          });
        }
        throw evalError;
      }
    } else {
      // Fallback: use EVAL directly if script not loaded
      console.warn(
        `⚠️ Script ${scriptName} not loaded, using EVAL directly...`
      );
      const scriptSource = getScriptSource(scriptName);
      return await redisClient.eval(scriptSource, {
        keys,
        arguments: args,
      });
    }
  } catch (error) {
    console.error(`❌ Error executing script ${scriptName}:`, error.message);
    throw error;
  }
};

/**
 * Get count with TTL refresh (atomic operation)
 * @param {string} key - Redis key
 * @param {number} ttl - TTL in seconds
 * @returns {string|null} Count value or null if not found
 */
export const getCountWithTTL = async (key, ttl) => {
  return await executeScript("getCountWithTTL", [key], [ttl.toString()]);
};

/**
 * Increment count with TTL refresh
 * @param {string} key - Redis key
 * @param {number} ttl - TTL in seconds
 * @returns {number} New count value
 */
export const incrementCountWithTTL = async (key, ttl) => {
  return await executeScript("incrementCountWithTTL", [key], [ttl.toString()]);
};

/**
 * Decrement count with TTL refresh and bounds checking
 * @param {string} key - Redis key
 * @param {number} ttl - TTL in seconds
 * @returns {number} New count value (never below 0)
 */
export const decrementCountWithTTL = async (key, ttl) => {
  return await executeScript("decrementCountWithTTL", [key], [ttl.toString()]);
};

/**
 * Add post to feed with trim and TTL (atomic operation)
 * @param {string} feedKey - Feed Redis key
 * @param {number} score - Timestamp score
 * @param {string} postId - Post ID
 * @param {number} maxFeedSize - Maximum feed size
 * @param {number} ttl - TTL in seconds
 * @returns {number} Number of elements added (1 or 0)
 */
export const addPostToFeedWithLua = async (
  feedKey,
  score,
  postId,
  maxFeedSize,
  ttl
) => {
  return await executeScript(
    "addPostToFeed",
    [feedKey],
    [score.toString(), postId, maxFeedSize.toString(), ttl.toString()]
  );
};

/**
 * Batch add post to multiple feeds using Redis pipelining
 * This reduces network round-trips by batching multiple LUA script executions
 * @param {Array<string>} feedKeys - Array of feed Redis keys
 * @param {number} score - Timestamp score
 * @param {string} postId - Post ID
 * @param {number} maxFeedSize - Maximum feed size
 * @param {number} ttl - TTL in seconds
 * @returns {Array} Array of results from each pipeline execution
 */
export const batchAddPostToFeeds = async (
  feedKeys,
  score,
  postId,
  maxFeedSize,
  ttl
) => {
  if (!feedKeys || feedKeys.length === 0) {
    return [];
  }

  try {
    const hash = scriptHashes.addPostToFeed;
    if (!hash) {
      // Fallback: load scripts if not loaded
      await loadLuaScripts();
    }

    const scriptHash = scriptHashes.addPostToFeed || hash;
    const scriptSource = getScriptSource("addPostToFeed");
    const args = [
      score.toString(),
      postId,
      maxFeedSize.toString(),
      ttl.toString(),
    ];

    // Use Promise.all with automatic pipelining in node-redis v4+
    // Commands issued in the same event loop tick are automatically batched
    // This is more reliable than manual pipelines and provides similar performance
    const promises = feedKeys.map((feedKey) => {
      if (scriptHash) {
        return redisClient.evalSha(scriptHash, {
          keys: [feedKey],
          arguments: args,
        });
      } else {
        return redisClient.eval(scriptSource, {
          keys: [feedKey],
          arguments: args,
        });
      }
    });

    // Promise.all will batch these automatically in node-redis v4+
    const results = await Promise.all(promises);
    return results;
  } catch (error) {
    console.error("❌ Error in batchAddPostToFeeds:", error.message);
    throw error;
  }
};

/**
 * Get feed with TTL refresh (atomic operation)
 * @param {string} feedKey - Feed Redis key
 * @param {number} start - Start index
 * @param {number} end - End index
 * @param {number} ttl - TTL in seconds
 * @returns {Array} Array of [value1, score1, value2, score2, ...]
 */
export const getFeedWithTTL = async (feedKey, start, end, ttl) => {
  return await executeScript(
    "getFeedWithTTL",
    [feedKey],
    [start.toString(), end.toString(), ttl.toString()]
  );
};

/**
 * Remove multiple posts from feed atomically
 * @param {string} feedKey - Feed Redis key
 * @param {Array<string>} postIds - Array of post IDs to remove
 * @returns {number} Number of elements removed
 */
export const removePostsFromFeedWithLua = async (feedKey, postIds) => {
  if (!postIds || postIds.length === 0) {
    return 0;
  }
  return await executeScript("removePostsFromFeed", [feedKey], postIds);
};

/**
 * Warm up feed cache with multiple posts (atomic batch operation)
 * @param {string} feedKey - Feed Redis key
 * @param {number} ttl - TTL in seconds
 * @param {number} maxFeedSize - Maximum feed size
 * @param {Array<{post_id: string, created_at: Date}>} feedItems - Array of feed items
 * @returns {number} Number of elements added
 */
export const warmUpFeedCacheWithLua = async (
  feedKey,
  ttl,
  maxFeedSize,
  feedItems
) => {
  if (!feedItems || feedItems.length === 0) {
    return 0;
  }

  // Build args array: [ttl, maxFeedSize, score1, postId1, score2, postId2, ...]
  const args = [ttl.toString(), maxFeedSize.toString()];
  feedItems.forEach((item) => {
    args.push(item.created_at.getTime().toString());
    args.push(item.post_id);
  });

  return await executeScript("warmUpFeedCache", [feedKey], args);
};

/**
 * Get feed with post details in one atomic LUA operation
 * This reduces 2 network round-trips to 1 by combining feed retrieval and post fetching
 * @param {string} feedKey - Feed Redis key
 * @param {number} start - Start index
 * @param {number} end - End index
 * @param {number} ttl - TTL in seconds
 * @returns {Object} { feedItems: [...], posts: [...] }
 */
export const getFeedWithPosts = async (feedKey, start, end, ttl) => {
  try {
    const result = await executeScript(
      "getFeedWithPosts",
      [feedKey],
      [start.toString(), end.toString(), ttl.toString()]
    );

    if (!result || !Array.isArray(result) || result.length < 2) {
      return { feedItems: [], posts: [] };
    }

    const [feedResult, postsResult] = result;

    // Parse feed items: [postId1, score1, postId2, score2, ...]
    const feedItems = [];
    if (feedResult && Array.isArray(feedResult)) {
      for (let i = 0; i < feedResult.length; i += 2) {
        feedItems.push({
          post_id: feedResult[i],
          created_at: new Date(parseInt(feedResult[i + 1])),
        });
      }
    }

    // Parse posts: [post1_json, post2_json, ...]
    const posts = [];
    if (postsResult && Array.isArray(postsResult)) {
      postsResult.forEach((postJson, index) => {
        if (postJson) {
          try {
            posts.push(JSON.parse(postJson));
          } catch (error) {
            console.error(
              `⚠️ Error parsing post ${feedItems[index]?.post_id}:`,
              error.message
            );
          }
        }
      });
    }

    return { feedItems, posts };
  } catch (error) {
    console.error("❌ Error in getFeedWithPosts:", error.message);
    throw error;
  }
};

/**
 * Get cached feed response (pre-serialized JSON)
 * This is the fastest path - single Redis GET + one JSON.parse
 * @param {string} feedKey - Feed Redis key
 * @param {number} ttl - TTL in seconds
 * @returns {string|null} Cached JSON string or null
 */
export const getCachedFeedResponse = async (feedKey, ttl) => {
  try {
    return await executeScript(
      "getCachedFeedResponse",
      [feedKey],
      [ttl.toString()]
    );
  } catch (error) {
    console.error("❌ Error getting cached feed response:", error.message);
    return null;
  }
};

/**
 * Cache complete feed response (pre-serialized)
 * This allows instant retrieval on next request
 * @param {string} feedKey - Feed Redis key
 * @param {Array} sortedPosts - Complete feed posts array
 * @param {number} ttl - TTL in seconds (default: 1 hour)
 */
export const cacheFeedResponse = async (feedKey, sortedPosts, ttl = 3600) => {
  try {
    const cachedResponseKey = `${feedKey}:response`;
    const value = JSON.stringify(sortedPosts);
    await redisClient.setEx(cachedResponseKey, ttl, value);
  } catch (error) {
    console.error("⚠️ Error caching feed response:", error.message);
  }
};

/**
 * Batch invalidate response caches for multiple users (atomic operation)
 * This is much more efficient than individual DEL operations
 * Only invalidates response cache, not feed cache (which is already updated)
 * @param {Array<number>} userIds - Array of user IDs
 * @returns {number} Number of keys deleted
 */
export const batchInvalidateResponseCaches = async (userIds) => {
  if (!userIds || userIds.length === 0) {
    return 0;
  }

  try {
    // Convert user IDs to strings for Lua script
    const args = userIds.map((id) => id.toString());
    const deleted = await executeScript(
      "batchInvalidateResponseCaches",
      [],
      args
    );
    return deleted;
  } catch (error) {
    console.error(
      "❌ Error batch invalidating response caches:",
      error.message
    );
    throw error;
  }
};

/**
 * Cache a single post in Redis
 * @param {string} postId - Post ID
 * @param {Object} postData - Post object to cache
 * @param {number} ttl - TTL in seconds (default: 1 hour)
 */
export const cachePost = async (postId, postData, ttl = 3600) => {
  try {
    const key = `post:${postId}`;
    const value = JSON.stringify(postData);
    await redisClient.setEx(key, ttl, value);
  } catch (error) {
    console.error(`⚠️ Error caching post ${postId}:`, error.message);
  }
};

/**
 * Batch get multiple posts from Redis cache
 * @param {Array<string>} postIds - Array of post IDs
 * @returns {Array} Array of post objects (null for cache misses)
 */
export const getPostsFromCache = async (postIds) => {
  if (!postIds || postIds.length === 0) {
    return [];
  }

  try {
    // Use MGET for batch retrieval (single network round-trip)
    const keys = postIds.map((id) => `post:${id}`);
    const values = await redisClient.mGet(keys);

    return values.map((value, index) => {
      if (value === null) return null;
      try {
        return JSON.parse(value);
      } catch (error) {
        console.error(
          `⚠️ Error parsing cached post ${postIds[index]}:`,
          error.message
        );
        return null;
      }
    });
  } catch (error) {
    console.error("❌ Error getting posts from cache:", error.message);
    return new Array(postIds.length).fill(null);
  }
};

/**
 * Batch cache multiple posts in Redis
 * @param {Array<Object>} posts - Array of post objects with id property
 * @param {number} ttl - TTL in seconds (default: 1 hour)
 */
export const batchCachePosts = async (posts, ttl = 3600) => {
  if (!posts || posts.length === 0) {
    return;
  }

  try {
    // Use Promise.all with automatic pipelining for batch writes
    const promises = posts.map((post) => {
      const key = `post:${post.id}`;
      const value = JSON.stringify(post);
      return redisClient.setEx(key, ttl, value);
    });

    await Promise.all(promises);
  } catch (error) {
    console.error("❌ Error batch caching posts:", error.message);
  }
};
