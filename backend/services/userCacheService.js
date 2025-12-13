import { redisClient } from "../config/db.js";
import { User } from "../models/index.js";
import {
  getCountWithTTL,
  incrementCountWithTTL,
  decrementCountWithTTL,
} from "./redisLuaScripts.js";

// Redis key patterns
const FOLLOWERS_COUNT_KEY = (userId) => `user:${userId}:followers_count`;
const FOLLOWING_COUNT_KEY = (userId) => `user:${userId}:following_count`;
const COUNT_TTL = 60 * 60; // 1 hour in seconds

/**
 * Get follower count from Redis cache or database
 * @param {number} userId - User ID
 * @returns {number} Follower count
 */
export const getFollowersCount = async (userId) => {
  try {
    const redisKey = FOLLOWERS_COUNT_KEY(userId);
    // Use LUA script for atomic GET + EXPIRE
    const cached = await getCountWithTTL(redisKey, COUNT_TTL);

    if (cached !== null) {
      // Cache hit - TTL already refreshed by LUA script
      return parseInt(cached, 10);
    }

    // Cache miss - get from database
    const user = await User.findByPk(userId, {
      attributes: ["followers_count"],
    });

    if (!user) {
      return 0;
    }

    // Warm up cache
    await redisClient.setEx(
      redisKey,
      COUNT_TTL,
      user.followers_count.toString()
    );

    return user.followers_count;
  } catch (error) {
    console.error(
      `⚠️ Error getting followers count for user ${userId}:`,
      error.message
    );
    // Fallback to database
    const user = await User.findByPk(userId, {
      attributes: ["followers_count"],
    });
    return user ? user.followers_count : 0;
  }
};

/**
 * Get following count from Redis cache or database
 * @param {number} userId - User ID
 * @returns {number} Following count
 */
export const getFollowingCount = async (userId) => {
  try {
    const redisKey = FOLLOWING_COUNT_KEY(userId);
    // Use LUA script for atomic GET + EXPIRE
    const cached = await getCountWithTTL(redisKey, COUNT_TTL);

    if (cached !== null) {
      // Cache hit - TTL already refreshed by LUA script
      return parseInt(cached, 10);
    }

    // Cache miss - get from database
    const user = await User.findByPk(userId, {
      attributes: ["following_count"],
    });

    if (!user) {
      return 0;
    }

    // Warm up cache
    await redisClient.setEx(
      redisKey,
      COUNT_TTL,
      user.following_count.toString()
    );

    return user.following_count;
  } catch (error) {
    console.error(
      `⚠️ Error getting following count for user ${userId}:`,
      error.message
    );
    // Fallback to database
    const user = await User.findByPk(userId, {
      attributes: ["following_count"],
    });
    return user ? user.following_count : 0;
  }
};

/**
 * Increment follower count in Redis and database
 * @param {number} userId - User ID
 */
export const incrementFollowersCount = async (userId) => {
  try {
    const redisKey = FOLLOWERS_COUNT_KEY(userId);
    // Use LUA script for atomic INCR + EXPIRE
    await incrementCountWithTTL(redisKey, COUNT_TTL);
  } catch (error) {
    console.error(
      `⚠️ Error incrementing followers count in Redis for user ${userId}:`,
      error.message
    );
  }
};

/**
 * Decrement follower count in Redis and database
 * @param {number} userId - User ID
 */
export const decrementFollowersCount = async (userId) => {
  try {
    const redisKey = FOLLOWERS_COUNT_KEY(userId);
    // Use LUA script for atomic DECR + bounds check + EXPIRE
    await decrementCountWithTTL(redisKey, COUNT_TTL);
  } catch (error) {
    console.error(
      `⚠️ Error decrementing followers count in Redis for user ${userId}:`,
      error.message
    );
  }
};

/**
 * Increment following count in Redis and database
 * @param {number} userId - User ID
 */
export const incrementFollowingCount = async (userId) => {
  try {
    const redisKey = FOLLOWING_COUNT_KEY(userId);
    // Use LUA script for atomic INCR + EXPIRE
    await incrementCountWithTTL(redisKey, COUNT_TTL);
  } catch (error) {
    console.error(
      `⚠️ Error incrementing following count in Redis for user ${userId}:`,
      error.message
    );
  }
};

/**
 * Decrement following count in Redis and database
 * @param {number} userId - User ID
 */
export const decrementFollowingCount = async (userId) => {
  try {
    const redisKey = FOLLOWING_COUNT_KEY(userId);
    // Use LUA script for atomic DECR + bounds check + EXPIRE
    await decrementCountWithTTL(redisKey, COUNT_TTL);
  } catch (error) {
    console.error(
      `⚠️ Error decrementing following count in Redis for user ${userId}:`,
      error.message
    );
  }
};

/**
 * Invalidate follower/following count cache for a user
 * @param {number} userId - User ID
 */
export const invalidateCountCache = async (userId) => {
  try {
    await redisClient.del(FOLLOWERS_COUNT_KEY(userId));
    await redisClient.del(FOLLOWING_COUNT_KEY(userId));
  } catch (error) {
    console.error(
      `⚠️ Error invalidating count cache for user ${userId}:`,
      error.message
    );
  }
};
