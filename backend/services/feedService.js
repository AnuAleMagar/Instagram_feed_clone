//sequelize feed service
import { Sequelize } from "sequelize"; // FIX: Import Sequelize class, not just sequelize instance
import { sequelize } from "../config/db.js";
import { Post } from "../models/index.js";
import { redisClient } from "../config/db.js";
import { Follow } from "../models/index.js";
import * as postService from "./postService.js";
import {
  addPostToFeedWithLua,
  getFeedWithTTL,
  removePostsFromFeedWithLua,
  warmUpFeedCacheWithLua,
  batchAddPostToFeeds,
  getCachedFeedResponse,
  batchInvalidateResponseCaches,
} from "./redisLuaScripts.js";
import { FEED_CONFIG } from "../config/constants.js";

// Redis key patterns
const FEED_KEY = (userId) => `feed:user:${userId}`;
const IDEMPOTENCY_KEY = (userId, postId) =>
  `fanout:idempotency:${userId}:${postId}`;
const MAX_FEED_SIZE = 100; // Top 100 posts per user in Redis
const FEED_TTL = FEED_CONFIG.FEED_TTL;
const FANOUT_BATCH_SIZE = FEED_CONFIG.FANOUT_BATCH_SIZE;

/**
 * Add a post to a user's feed in Redis only
 * @param {number} userId - User ID whose feed to update
 * @param {string} postId - Post UUID string
 * @param {Date} createdAt - Post creation timestamp
 */
export const addPostToFeedRedis = async (userId, postId, createdAt) => {
  try {
    const redisKey = FEED_KEY(userId);
    const score = createdAt.getTime(); // Convert to milliseconds for sorting

    // Use LUA script for atomic ZADD + ZREMRANGEBYRANK + EXPIRE
    // This keeps only the top 100 posts (MAX_FEED_SIZE)
    await addPostToFeedWithLua(
      redisKey,
      score,
      postId,
      MAX_FEED_SIZE,
      FEED_TTL
    );
  } catch (error) {
    console.error(
      `⚠️ Redis feed write error for user ${userId}:`,
      error.message
    );
    throw error; // Re-throw so Kafka retry mechanism can handle it
  }
};

/**
 * Fan-out: Add post to all followers' feeds in Redis only
 * This is called by Kafka consumer after post creation
 * @param {number} userId - User who created the post
 * @param {string} postId - Post UUID string
 * @param {Date} createdAt - Post creation timestamp
 */
export const fanOutToFollowers = async (userId, postId, createdAt) => {
  try {
    // Validate inputs
    if (!userId || !postId) {
      throw new Error("userId and postId are required");
    }

    // Validate and normalize createdAt
    let validCreatedAt;
    if (createdAt instanceof Date) {
      validCreatedAt = createdAt;
    } else if (typeof createdAt === "string") {
      validCreatedAt = new Date(createdAt);
    } else {
      validCreatedAt = new Date();
    }

    if (isNaN(validCreatedAt.getTime())) {
      console.error(
        `⚠️ Invalid createdAt date: ${createdAt}, using current time`
      );
      validCreatedAt = new Date();
    }

    // Get all followers of the user who posted
    const followers = await Follow.findAll({
      where: {
        following_id: userId,
      },
      attributes: ["follower_id"],
    });

    if (followers.length === 0) {
      return;
    }

    // Calculate score safely
    const score = validCreatedAt.getTime();
    if (!Number.isFinite(score) || score < 0) {
      console.error(
        `⚠️ Invalid score calculated: ${score}, using current time`
      );
      validCreatedAt = new Date();
    }

    const followerIds = followers.map((follow) => follow.follower_id);
    const totalFollowers = followerIds.length;

    // Process in batches to prevent OOM with large follower lists
    const batches = [];
    for (let i = 0; i < followerIds.length; i += FANOUT_BATCH_SIZE) {
      batches.push(followerIds.slice(i, i + FANOUT_BATCH_SIZE));
    }

    console.log(
      `📊 [FAN-OUT] Processing ${totalFollowers} followers in ${batches.length} batches`
    );

    // Process each batch sequentially to prevent memory issues
    for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
      const batch = batches[batchIndex];
      const feedKeys = batch.map((id) => FEED_KEY(id));

      try {
        // Batch update Redis feeds
        await batchAddPostToFeeds(
          feedKeys,
          score,
          postId,
          MAX_FEED_SIZE,
          FEED_TTL
        ).catch((err) => {
          console.error(
            `⚠️ Redis batch update failed for batch ${batchIndex + 1}:`,
            err.message
          );
          // Fallback to individual writes
          return Promise.allSettled(
            batch.map((followerId) =>
              addPostToFeedRedis(followerId, postId, validCreatedAt)
            )
          );
        });

        // CRITICAL: Invalidate response caches AFTER adding post to feed
        // This ensures the next request will get fresh data from Redis feed
        const invalidationResult = await batchInvalidateResponseCaches(batch);
        console.log(
          `🗑️ [CACHE] Invalidated response caches for ${invalidationResult} users in batch ${
            batchIndex + 1
          }`
        );

        console.log(
          `✅ [FAN-OUT] Batch ${batchIndex + 1}/${batches.length} completed (${
            batch.length
          } followers)`
        );
      } catch (error) {
        console.error(
          `❌ [FAN-OUT] Error processing batch ${batchIndex + 1}:`,
          error.message
        );
        // Continue with next batch - don't fail entire fan-out
      }
    }

    console.log(
      `✅ [FAN-OUT] Completed fan-out to ${totalFollowers} followers`
    );
  } catch (error) {
    console.error(`❌ [FAN-OUT] Critical error:`, error.message, error.stack);
    throw error; // Re-throw so Kafka retry mechanism can handle it
  }
};

/**
 * Backfill: Add all existing posts from a user to a follower's feed in Redis
 * Used when a new user follows someone who already has posts
 * @param {number} followerId - User who just started following
 * @param {number} followingId - User being followed
 */
export const backfillFeedOnFollow = async (followerId, followingId) => {
  try {
    // Get existing posts from the user being followed (from PostgreSQL)
    // FIX: Limit to top MAX_FEED_SIZE posts to prevent adding too many
    const existingPosts = await postService.getPostsByUser(followingId, {
      limit: MAX_FEED_SIZE,
      order: [["created_at", "DESC"]],
    });

    if (existingPosts.length === 0) {
      return 0;
    }

    // Get current feed from Redis to check which posts are already there
    const currentFeed = await getFeedFromRedis(followerId, MAX_FEED_SIZE);
    const existingPostIds = new Set(
      currentFeed.map((item) => item.post_id.toString())
    );

    // Filter out posts that are already in the feed
    const postsToAdd = existingPosts.filter(
      (post) => !existingPostIds.has(post.id.toString())
    );

    if (postsToAdd.length === 0) {
      return 0;
    }

    // FIX: Add posts one by one (each addition respects MAX_FEED_SIZE via LUA script)
    // Sort by created_at DESC to add newest posts first
    const sortedPosts = postsToAdd.sort((a, b) => {
      const dateA =
        a.created_at instanceof Date ? a.created_at : new Date(a.created_at);
      const dateB =
        b.created_at instanceof Date ? b.created_at : new Date(b.created_at);
      return dateB.getTime() - dateA.getTime();
    });

    // Add posts sequentially to ensure MAX_FEED_SIZE limit is respected
    for (const post of sortedPosts) {
      await addPostToFeedRedis(followerId, post.id, post.created_at);
    }

    // Invalidate cached response so new posts appear immediately
    await invalidateFeedCache(followerId);

    return postsToAdd.length;
  } catch (error) {
    console.error(
      `⚠️ Error backfilling feed for follower ${followerId}:`,
      error.message
    );
    throw error;
  }
};

/**
 * Remove all posts from a specific user from a follower's feed in Redis
 * Used when a user unfollows someone
 * @param {number} followerId - User who unfollowed
 * @param {number} unfollowedId - User who was unfollowed
 */
export const removePostsFromFeedOnUnfollow = async (
  followerId,
  unfollowedId
) => {
  try {
    // Get all posts from the unfollowed user (from PostgreSQL)
    const postsToRemove = await postService.getPostsByUser(unfollowedId);

    if (postsToRemove.length === 0) {
      return 0;
    }

    const postIds = postsToRemove.map((post) => post.id);

    // Remove posts from Redis cache using LUA script (atomic batch operation)
    const redisKey = FEED_KEY(followerId);
    await removePostsFromFeedWithLua(redisKey, postIds);

    // Clear idempotency markers for removed posts
    const idempotencyDeletePromises = postIds.map(async (postId) => {
      try {
        const idempotencyKey = IDEMPOTENCY_KEY(followerId, postId);
        await redisClient.del(idempotencyKey);
      } catch (error) {
        // Non-critical - log but don't fail
        console.error(
          `⚠️ Error clearing idempotency marker for post ${postId}:`,
          error.message
        );
      }
    });
    await Promise.allSettled(idempotencyDeletePromises);

    // Invalidate cached response
    await invalidateFeedCache(followerId);

    return postsToRemove.length;
  } catch (error) {
    console.error(
      `⚠️ Error removing posts from feed for follower ${followerId}:`,
      error.message
    );
    throw error;
  }
};

/**
 * Get user's feed from Redis (top 100 posts)
 * @param {number} userId - User ID
 * @param {number} limit - Maximum number of posts to return
 * @returns {Array} Array of post IDs with timestamps
 */
export const getFeedFromRedis = async (userId, limit = 20) => {
  try {
    const redisKey = FEED_KEY(userId);
    const limitInt = parseInt(limit);

    // Use LUA script for atomic ZREVRANGE + EXPIRE
    const result = await getFeedWithTTL(redisKey, 0, limitInt - 1, FEED_TTL);

    if (result && result.length > 0) {
      // Cache hit! TTL already refreshed by LUA script
      // Parse result: [value1, score1, value2, score2, ...]
      const feedItems = [];
      for (let i = 0; i < result.length; i += 2) {
        feedItems.push({
          post_id: result[i],
          created_at: new Date(parseInt(result[i + 1])), // Convert milliseconds back to Date
        });
      }

      return feedItems;
    }

    return [];
  } catch (error) {
    console.error(
      `⚠️ Redis feed read error for user ${userId}:`,
      error.message
    );
    return [];
  }
};

/**
 * Get user's feed from PostgreSQL with cursor-based pagination
 * Used when requesting posts beyond the first 100 (Redis limit)
 * @param {number} userId - User ID
 * @param {number} limit - Maximum number of posts to return
 * @param {Date} cursor - Cursor timestamp (posts before this date)
 * @returns {Array} Array of posts
 */
export const getFeedFromPostgres = async (
  userId,
  limit = 20,
  cursor = null
) => {
  try {
    const userIdInt = parseInt(userId);
    const limitInt = parseInt(limit);

    // Get all users that this user follows
    const following = await Follow.findAll({
      where: {
        follower_id: userIdInt,
      },
      attributes: ["following_id"],
    });

    if (following.length === 0) {
      return [];
    }

    const followingIds = following.map((f) => f.following_id);

    // Parse cursor that might include post ID
    let cursorDate = null;
    let excludePostId = null;

    if (cursor) {
      // Check if cursor includes post ID (format: "2025-12-05T12:07:57.610Z_post-id")
      if (typeof cursor === "string" && cursor.includes("_")) {
        const parts = cursor.split("_");
        cursorDate = new Date(parts[0]);
        excludePostId = parts.slice(1).join("_"); // In case UUID has underscores
      } else {
        cursorDate = cursor instanceof Date ? cursor : new Date(cursor);
      }

      if (isNaN(cursorDate.getTime())) {
        console.error(`⚠️ Invalid cursor: ${cursor}`);
        return [];
      }
    }

    // Build query with cursor-based pagination
    // FIX: Build whereClause correctly - use explicit structure
    let whereClause;

    if (cursor && cursorDate) {
      // When cursor is provided, build complete where clause with AND/OR structure
      whereClause = {
        [Sequelize.Op.and]: [
          // FIX: Use Sequelize.Op instead of sequelize.Op
          { user_id: { [Sequelize.Op.in]: followingIds } },
          {
            [Sequelize.Op.or]: [
              { created_at: { [Sequelize.Op.lt]: cursorDate } },
              {
                [Sequelize.Op.and]: [
                  { created_at: { [Sequelize.Op.eq]: cursorDate } },
                  { id: { [Sequelize.Op.ne]: excludePostId } },
                ],
              },
            ],
          },
        ],
      };
    } else {
      // No cursor - simple query
      whereClause = {
        user_id: {
          [Sequelize.Op.in]: followingIds, // FIX: Use Sequelize.Op
        },
      };
    }

    const posts = await Post.findAll({
      where: whereClause,
      order: [["created_at", "DESC"]],
      limit: limitInt,
    });

    console.log(
      `📊 [POSTGRES] User ${userId}: Found ${posts.length} posts from postgres`
    );

    return posts.map((post) => ({
      id: post.id,
      user_id: post.user_id,
      caption: post.caption,
      image_url: post.image_url,
      likes_count: post.likes_count,
      comments_count: post.comments_count,
      created_at: post.created_at,
    }));
  } catch (error) {
    console.error(
      `⚠️ PostgreSQL feed read error for user ${userId}:`,
      error.message,
      error.stack
    );
    return [];
  }
};

/**
 * Rebuild user's feed in Redis from PostgreSQL
 * Used when Redis feed is empty (cache miss)
 * @param {number} userId - User ID
 */
export const rebuildFeedFromPostgres = async (userId) => {
  try {
    // Get all users that this user follows
    const following = await Follow.findAll({
      where: {
        follower_id: userId,
      },
      attributes: ["following_id"],
    });

    if (following.length === 0) {
      return [];
    }

    const followingIds = following.map((f) => f.following_id);

    // Get top 100 posts from followed users (ordered by created_at DESC)
    const posts = await Post.findAll({
      where: {
        user_id: followingIds,
      },
      order: [["created_at", "DESC"]],
      limit: MAX_FEED_SIZE,
    });

    if (posts.length === 0) {
      return [];
    }

    // Convert posts to feed items format
    const feedItems = posts.map((post) => ({
      post_id: post.id,
      created_at: post.created_at,
    }));

    // Warm up Redis cache with these posts
    await warmUpCache(userId, feedItems);

    return feedItems;
  } catch (error) {
    console.error(
      `⚠️ Error rebuilding feed from PostgreSQL for user ${userId}:`,
      error.message
    );
    return [];
  }
};

/**
 * Warm up Redis cache with feed data
 * @param {number} userId - User ID
 * @param {Array} feedItems - Array of {post_id, created_at}
 */
export const warmUpCache = async (userId, feedItems) => {
  if (!feedItems || feedItems.length === 0) return;

  try {
    const redisKey = FEED_KEY(userId);
    // Use LUA script for atomic batch operation (ZADD + ZREMRANGEBYRANK + EXPIRE)
    await warmUpFeedCacheWithLua(redisKey, FEED_TTL, MAX_FEED_SIZE, feedItems);
  } catch (error) {
    console.error(`⚠️ Cache warm-up error for user ${userId}:`, error.message);
  }
};

/**
 * Get user's feed (hybrid: Redis first 100, PostgreSQL for beyond 100)
 * FIX: Support querying limit + 1 for accurate has_more calculation
 * FIX: Hybrid approach - use Redis when cursor is within Redis, PostgreSQL for remaining
 * @param {number} userId - User ID
 * @param {number} limit - Maximum number of posts to return (may be limit + 1 for has_more check)
 * @param {Date} cursor - Cursor for pagination (optional)
 * @returns {Array} Array of posts
 */
export const getFeed = async (userId, limit = 20, cursor = null) => {
  const limitInt = parseInt(limit);

  // If requesting more than 100 posts, always use PostgreSQL
  if (limitInt > MAX_FEED_SIZE) {
    const posts = await getFeedFromPostgres(userId, limitInt, cursor);
    console.log(
      `📊 [FEED SOURCE] User ${userId}: ${posts.length} from postgres (limit > 100)`
    );
    return posts;
  }

  // If cursor provided, check if we can use Redis (hybrid approach)
  if (cursor) {
    // Parse cursor to extract date and post ID
    let cursorDate = null;
    let cursorPostId = null;

    if (typeof cursor === "string" && cursor.includes("_")) {
      const parts = cursor.split("_");
      cursorDate = new Date(parts[0]);
      cursorPostId = parts.slice(1).join("_"); // Handle UUIDs that might have underscores
    } else {
      cursorDate = cursor instanceof Date ? cursor : new Date(cursor);
    }

    if (isNaN(cursorDate.getTime())) {
      console.error(
        `⚠️ Invalid cursor date: ${cursor}, falling back to PostgreSQL`
      );
      return await getFeedFromPostgres(userId, limitInt, cursor);
    }

    // Get all available posts from Redis (up to MAX_FEED_SIZE)
    const redisFeed = await getFeedFromRedis(userId, MAX_FEED_SIZE);

    // Check if cursor post exists in Redis feed
    const cursorPostInRedis =
      cursorPostId &&
      redisFeed.some(
        (item) => item.post_id.toString() === cursorPostId.toString()
      );

    if (cursorPostInRedis && redisFeed.length > 0) {
      // Cursor post is in Redis - filter posts after cursor from Redis
      const filteredRedisFeed = redisFeed.filter((item) => {
        const itemDate =
          item.created_at instanceof Date
            ? item.created_at
            : new Date(item.created_at);
        const itemTimestamp = itemDate.getTime();
        const cursorTimestamp = cursorDate.getTime();

        // Include posts with created_at < cursor date
        // Or same timestamp but different post ID (to exclude cursor post)
        return (
          itemTimestamp < cursorTimestamp ||
          (itemTimestamp === cursorTimestamp &&
            item.post_id.toString() !== cursorPostId.toString())
        );
      });

      // If Redis has enough posts, return from Redis only
      if (filteredRedisFeed.length >= limitInt) {
        const limitedFeed = filteredRedisFeed.slice(0, limitInt);
        const postIds = limitedFeed.map((item) => item.post_id);
        const posts = await postService.getPostsByIds(postIds);

        // Return posts in the same order as feed
        const postsMap = new Map(posts.map((post) => [post.id, post]));
        const result = limitedFeed
          .map((item) => postsMap.get(item.post_id))
          .filter((post) => post !== undefined);

        console.log(
          `📊 [FEED SOURCE] User ${userId}: ${result.length} from redis`
        );
        return result;
      }

      // Redis doesn't have enough posts - hybrid approach
      // Get all available from Redis, then get remaining from PostgreSQL
      const redisPosts = filteredRedisFeed;
      const remainingNeeded = limitInt - redisPosts.length;

      console.log(
        `🔄 [HYBRID] User ${userId}: Redis has ${redisPosts.length} posts, need ${limitInt} total. Getting ${remainingNeeded} more from PostgreSQL.`
      );

      // Get remaining posts from PostgreSQL using cursor from last Redis post
      const lastRedisPost = redisPosts[redisPosts.length - 1];
      const lastPostDate =
        lastRedisPost.created_at instanceof Date
          ? lastRedisPost.created_at
          : new Date(lastRedisPost.created_at);
      const pgCursor = `${lastPostDate.toISOString()}_${lastRedisPost.post_id}`;

      const pgPosts = await getFeedFromPostgres(
        userId,
        remainingNeeded,
        pgCursor
      );

      // Get post details for Redis posts
      const redisPostIds = redisPosts.map((item) => item.post_id);
      const allPostIds = [...redisPostIds, ...pgPosts.map((p) => p.id)];
      const allPosts = await postService.getPostsByIds(allPostIds);

      // Combine Redis + PostgreSQL results
      const postsMap = new Map(allPosts.map((post) => [post.id, post]));
      const redisPostDetails = redisPosts
        .map((item) => postsMap.get(item.post_id))
        .filter((post) => post !== undefined);

      const combinedFeed = [...redisPostDetails, ...pgPosts];

      console.log(
        `📊 [FEED SOURCE] User ${userId}: ${redisPostDetails.length} from redis + ${pgPosts.length} from postgres = ${combinedFeed.length} total`
      );

      return combinedFeed;
    }

    // Cursor post not in Redis - must be beyond 100, use PostgreSQL
    const posts = await getFeedFromPostgres(userId, limitInt, cursor);
    console.log(
      `📊 [FEED SOURCE] User ${userId}: ${posts.length} from postgres (cursor beyond redis)`
    );
    return posts;
  }

  // No cursor - try Redis first (top 100 posts)
  const cachedFeed = await getFeedFromRedis(userId, limitInt);

  if (cachedFeed && cachedFeed.length > 0) {
    // Cache hit - get post details from PostgreSQL
    const postIds = cachedFeed.map((item) => item.post_id);
    const posts = await postService.getPostsByIds(postIds);

    // Return posts in the same order as feed
    const postsMap = new Map(posts.map((post) => [post.id, post]));
    const result = cachedFeed
      .map((item) => postsMap.get(item.post_id))
      .filter((post) => post !== undefined);

    console.log(
      `📊 [FEED SOURCE] User ${userId}: ${result.length} from redis (first page)`
    );
    return result;
  }

  // Cache miss - rebuild from PostgreSQL
  const feedItems = await rebuildFeedFromPostgres(userId);

  if (feedItems.length === 0) {
    return [];
  }

  // Get post details from PostgreSQL
  const postIds = feedItems.map((item) => item.post_id);
  const posts = await postService.getPostsByIds(postIds);

  // Return posts in the same order as feed
  const postsMap = new Map(posts.map((post) => [post.id, post]));
  const result = feedItems
    .map((item) => postsMap.get(item.post_id))
    .filter((post) => post !== undefined);

  console.log(
    `📊 [FEED SOURCE] User ${userId}: ${result.length} from postgres (cache miss)`
  );
  return result;
};

/**
 * Get cached feed response (pre-serialized JSON) - fastest path
 * @param {number} userId - User ID
 * @param {number} limit - Maximum number of posts (for logging)
 * @returns {Array|null} Cached feed posts array or null if not cached
 */
export const getFeedResponseFromCache = async (userId, limit = 20) => {
  try {
    const redisKey = FEED_KEY(userId);
    const cached = await getCachedFeedResponse(redisKey, FEED_TTL);

    if (cached) {
      return JSON.parse(cached);
    }

    return null;
  } catch (error) {
    console.error(
      `⚠️ Error getting cached feed response for user ${userId}:`,
      error.message
    );
    return null;
  }
};

/**
 * Invalidate user's feed cache (e.g., on unfollow)
 * @param {number} userId - User ID
 */
export const invalidateFeedCache = async (userId) => {
  try {
    const redisKey = FEED_KEY(userId);
    const cachedResponseKey = `${redisKey}:response`;

    // Delete both feed cache and response cache
    await Promise.all([
      redisClient.del(redisKey),
      redisClient.del(cachedResponseKey),
    ]);
  } catch (error) {
    console.error(
      `⚠️ Cache invalidation error for user ${userId}:`,
      error.message
    );
  }
};

/**
 * Check if there are more posts available after the given post
 * @param {number} userId - User ID
 * @param {number} limit - Limit to check
 * @param {Object} lastPost - Last post in current feed
 * @returns {boolean} True if there are more posts
 */
export const checkHasMorePosts = async (userId, limit, lastPost) => {
  try {
    if (!lastPost) return false;

    // If we're using Redis and have less than MAX_FEED_SIZE posts, no more in Redis
    const redisFeed = await getFeedFromRedis(userId, MAX_FEED_SIZE);
    if (redisFeed.length < MAX_FEED_SIZE) {
      // Check PostgreSQL for more posts
      const cursor =
        lastPost.created_at instanceof Date
          ? lastPost.created_at
          : new Date(lastPost.created_at);
      const nextPosts = await getFeedFromPostgres(
        userId,
        1,
        `${cursor.toISOString()}_${lastPost.id}`
      );
      return nextPosts.length > 0;
    }

    // If Redis has MAX_FEED_SIZE posts, check PostgreSQL for more
    const cursor =
      lastPost.created_at instanceof Date
        ? lastPost.created_at
        : new Date(lastPost.created_at);
    const nextPosts = await getFeedFromPostgres(
      userId,
      1,
      `${cursor.toISOString()}_${lastPost.id}`
    );
    return nextPosts.length > 0;
  } catch (error) {
    console.error(
      `⚠️ Error checking has_more for user ${userId}:`,
      error.message
    );
    return false; // Conservative: assume no more posts on error
  }
};
