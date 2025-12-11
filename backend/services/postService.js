import { Post } from "../models/index.js"; // Use Sequelize Post model
import { randomUUID } from "crypto";
import { sendMessage } from "./kafkaProducer.js";
import { TOPICS } from "../config/kafka.js";
import {
  getPostsFromCache,
  batchCachePosts,
  cachePost,
} from "./redisLuaScripts.js";

/**
 * Create a new post
 * @param {Object} postData - { user_id, caption, image_url, created_at }
 * @returns {Object} Created post
 */
export const createPost = async (postData) => {
  const { user_id, caption, image_url, created_at } = postData;

  if (!user_id || !image_url) {
    throw new Error("user_id and image_url are required");
  }

  const userId = parseInt(user_id);
  if (isNaN(userId)) {
    throw new Error("user_id must be a valid number");
  }

  // STEP 1: Generate UUID client-side (before any DB operation)
  const postId = randomUUID();
  const createdAt = created_at ? new Date(created_at) : new Date();

  // STEP 2: Start PostgreSQL write (use raw: true for faster response)
  const postgresPromise = Post.create(
    {
      id: postId,
      user_id: userId,
      caption: caption || null,
      image_url: image_url,
      likes_count: 0,
      comments_count: 0,
      created_at: createdAt,
    },
    {
      raw: false, // Keep false to get model instance for toJSON()
    }
  );

  // STEP 3: Check Kafka availability BEFORE creating promise (optimization)
  const { isKafkaAvailable } = await import("../config/kafka.js");

  // Start Kafka publish immediately (parallel, non-blocking) - only if available
  let kafkaPromise = Promise.resolve();
  if (isKafkaAvailable()) {
    kafkaPromise = sendMessage(
      TOPICS.POST_CREATED,
      {
        eventType: "POST_CREATED",
        postId: postId,
        userId: userId,
        createdAt: createdAt.toISOString(),
      },
      userId.toString() // Key for partitioning
    ).catch((error) => {
      // Log but don't throw - Kafka failure shouldn't break post creation
      console.error("⚠️ Failed to publish post to Kafka:", error.message);
    });
  }

  // STEP 4: Wait ONLY for PostgreSQL success (Kafka continues in background)
  const post = await postgresPromise;

  // STEP 5: Return using toJSON() for faster serialization
  return post.toJSON();
};

/**
 * Get post by ID (from PostgreSQL with Redis cache)
 * @param {string} postId - UUID string
 * @returns {Object|null} Post object or null if not found
 */
export const getPostById = async (postId) => {
  // Check Redis cache first
  const cachedPosts = await getPostsFromCache([postId]);
  if (cachedPosts[0]) {
    return cachedPosts[0];
  }

  // Query PostgreSQL
  const post = await Post.findByPk(postId);

  if (!post) {
    return null;
  }

  const postData = {
    id: post.id,
    user_id: post.user_id,
    caption: post.caption,
    image_url: post.image_url,
    likes_count: post.likes_count,
    comments_count: post.comments_count,
    created_at: post.created_at,
  };

  // Cache the post
  await cachePost(postId, postData).catch((err) =>
    console.error("Failed to cache post:", err)
  );

  return postData;
};

/**
 * Get all posts by a user (from PostgreSQL)
 * @param {number} userId - User ID
 * @returns {Array} Array of posts
 */
export const getPostsByUser = async (userId, options = {}) => {
  const { limit, order = [["created_at", "DESC"]] } = options;

  const queryOptions = {
    where: { user_id: userId },
    order: order,
  };

  if (limit) {
    queryOptions.limit = limit;
  }

  return await Post.findAll(queryOptions);
};

/**
 * Get multiple posts by their IDs (batch fetch with Redis cache)
 * @param {Array<string>} postIds - Array of UUID strings
 * @returns {Array} Array of posts
 */
export const getPostsByIds = async (postIds) => {
  if (!postIds || postIds.length === 0) {
    return [];
  }

  // Check Redis cache first
  const cachedPosts = await getPostsFromCache(postIds);
  const cachedMap = new Map();
  const missingIds = [];

  cachedPosts.forEach((post, index) => {
    if (post) {
      cachedMap.set(postIds[index], post);
    } else {
      missingIds.push(postIds[index]);
    }
  });

  if (missingIds.length === 0) {
    return postIds.map((id) => cachedMap.get(id));
  }

  // Query PostgreSQL for missing posts
  const posts = await Post.findAll({
    where: {
      id: missingIds,
    },
  });

  const fetchedPosts = posts.map((post) => ({
    id: post.id,
    user_id: post.user_id,
    caption: post.caption,
    image_url: post.image_url,
    likes_count: post.likes_count,
    comments_count: post.comments_count,
    created_at: post.created_at,
  }));

  // Cache the fetched posts
  if (fetchedPosts.length > 0) {
    await batchCachePosts(fetchedPosts);
  }

  fetchedPosts.forEach((post) => cachedMap.set(post.id, post));

  return postIds.map((id) => cachedMap.get(id));
};

/**
 * Get all posts (limited)
 * @param {number} limit - Maximum number of posts to return
 * @returns {Array} Array of posts
 */
export const getAllPosts = async (limit = 50) => {
  const limitInt = parseInt(limit);
  const posts = await Post.findAll({
    order: [["created_at", "DESC"]],
    limit: limitInt,
  });

  return posts.map((post) => ({
    id: post.id,
    user_id: post.user_id,
    caption: post.caption,
    image_url: post.image_url,
    likes_count: post.likes_count,
    comments_count: post.comments_count,
    created_at: post.created_at,
  }));
};
