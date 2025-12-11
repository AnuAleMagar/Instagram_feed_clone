import * as postService from "../services/postService.js";
import * as feedService from "../services/feedService.js";
import { cacheFeedResponse } from "../services/redisLuaScripts.js";

/**
 * Create a new post
 * POST /api/posts
 */
export const createPost = async (req, res) => {
  try {
    const { user_id, caption, image_url, created_at } = req.body;

    // Fast validation - return early
    if (!user_id || !image_url) {
      return res.status(400).json({
        success: false,
        message: "user_id and image_url are required",
      });
    }

    // Create post using service (service handles PostgreSQL + Kafka)
    const post = await postService.createPost({
      user_id,
      caption,
      image_url,
      created_at,
    });

    // Return immediately - use 201 status
    res.status(201).json({
      success: true,
      message: "Post created successfully",
      post,
    });
  } catch (error) {
    console.error("Error creating post:", error);
    res.status(500).json({
      success: false,
      message: "Error creating post",
      error: error.message,
    });
  }
};

/**
 * Get post by ID
 * GET /api/posts/:id
 */
export const getPostById = async (req, res) => {
  try {
    const { id } = req.params;
    const post = await postService.getPostById(id);

    if (!post) {
      return res.status(404).json({
        success: false,
        message: "Post not found",
      });
    }

    res.status(200).json({
      success: true,
      post,
    });
  } catch (error) {
    console.error("Error fetching post:", error);
    res.status(500).json({
      success: false,
      message: "Error fetching post",
      error: error.message,
    });
  }
};

/**
 * Get all posts by a user
 * GET /api/posts/user/:user_id
 */
export const getPostsByUser = async (req, res) => {
  try {
    const { user_id } = req.params;
    const posts = await postService.getPostsByUser(user_id);

    res.status(200).json({
      success: true,
      posts,
      count: posts.length,
    });
  } catch (error) {
    console.error("Error fetching user posts:", error);
    res.status(500).json({
      success: false,
      message: "Error fetching user posts",
      error: error.message,
    });
  }
};

/**
 * Get all posts (limited)
 * GET /api/posts
 */
export const getAllPosts = async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 50;
    const posts = await postService.getAllPosts(limit);

    res.status(200).json({
      success: true,
      posts,
      count: posts.length,
    });
  } catch (error) {
    console.error("Error fetching posts:", error);
    res.status(500).json({
      success: false,
      message: "Error fetching posts",
      error: error.message,
    });
  }
};

/**
 * Get user's feed
 * GET /api/posts/feed/:user_id
 */
export const getUserFeed = async (req, res) => {
  try {
    const { user_id } = req.params;
    const limit = parseInt(req.query.limit) || 20;
    const refresh = req.query.refresh === "true" || req.query.refresh === "1";

    // FIX: Better cursor parsing with validation - handle format with post ID
    let cursor = null;
    if (req.query.cursor) {
      const cursorString = req.query.cursor;

      // Check if cursor includes post ID (format: "2025-12-05T12:07:57.610Z_post-id")
      if (cursorString.includes("_")) {
        const parts = cursorString.split("_");
        const datePart = parts[0];
        const dateObj = new Date(datePart);

        if (isNaN(dateObj.getTime())) {
          return res.status(400).json({
            success: false,
            message: "Invalid cursor format. Date part is invalid.",
          });
        }

        // Keep cursor as string (feedService will parse it)
        cursor = cursorString;
        console.log(
          `🔍 [PAGINATION] User ${user_id}: Using cursor with post ID=${cursor}`
        );
      } else {
        // Old format - just date
        const dateObj = new Date(cursorString);
        if (isNaN(dateObj.getTime())) {
          return res.status(400).json({
            success: false,
            message: "Invalid cursor format. Use ISO 8601 date string.",
          });
        }
        cursor = cursorString; // Keep as string
        console.log(
          `🔍 [PAGINATION] User ${user_id}: Using cursor=${dateObj.toISOString()}`
        );
      }
    }

    // Validate user_id
    const userIdInt = parseInt(user_id);
    if (isNaN(userIdInt)) {
      return res.status(400).json({
        success: false,
        message: "Invalid user_id",
      });
    }

    // FIX: Skip cache if cursor is provided (pagination requires fresh data)
    if (!refresh && !cursor) {
      const minCachedPosts = Math.max(1, Math.floor(limit * 0.8));
      const cachedResponse = await feedService.getFeedResponseFromCache(
        user_id,
        limit
      );

      if (cachedResponse && cachedResponse.length >= minCachedPosts) {
        // FIX: Slice cached response to requested limit
        const trimmedCachedResponse = cachedResponse.slice(0, limit);

        // FIX: Validate cache freshness - check if Redis feed has newer posts
        try {
          const redisFeed = await feedService.getFeedFromRedis(user_id, 1); // Get just the newest post from Redis

          // DEBUG: Log what we found
          console.log(
            `🔍 [CACHE VALIDATION] User ${user_id}: Redis feed length=${
              redisFeed?.length || 0
            }, Cache length=${
              cachedResponse.length
            }, Requested limit=${limit}, Trimmed to=${
              trimmedCachedResponse.length
            }`
          );

          if (
            redisFeed &&
            redisFeed.length > 0 &&
            trimmedCachedResponse.length > 0
          ) {
            const newestPostInRedis = redisFeed[0];
            const newestPostInCache = trimmedCachedResponse[0];

            // FIX: Convert post_id to string for comparison (UUIDs are strings)
            const redisPostId = newestPostInRedis.post_id.toString();
            const cachePostId = newestPostInCache.id.toString();

            // Compare timestamps - if Redis has a newer post, cache is stale
            const redisTimestamp = new Date(
              newestPostInRedis.created_at
            ).getTime();
            const cacheTimestamp = new Date(
              newestPostInCache.created_at
            ).getTime();

            console.log(
              `🔍 [CACHE VALIDATION] User ${user_id}: Redis timestamp=${redisTimestamp}, Cache timestamp=${cacheTimestamp}, Redis post_id=${redisPostId}, Cache post_id=${cachePostId}`
            );

            // FIX: Check if Redis has a newer post OR if post IDs don't match
            if (
              redisTimestamp > cacheTimestamp ||
              redisPostId !== cachePostId
            ) {
              // Cache is stale - invalidate it and rebuild
              console.log(
                `🔄 [CACHE] Stale cache detected for user ${user_id}. Redis newest: ${newestPostInRedis.created_at} (${redisPostId}), Cache newest: ${newestPostInCache.created_at} (${cachePostId})`
              );
              await feedService.invalidateFeedCache(user_id);
              // Fall through to rebuild from Redis feed below
            } else {
              // Cache is fresh, return it (trimmed to limit)
              console.log(`✅ [CACHE] Cache is fresh for user ${user_id}`);

              // FIX: Calculate pagination info for cached response too
              const hasMore = await feedService.checkHasMorePosts(
                user_id,
                limit,
                trimmedCachedResponse[trimmedCachedResponse.length - 1]
              );

              const lastPost =
                trimmedCachedResponse[trimmedCachedResponse.length - 1];
              let nextCursor = null;
              if (lastPost && lastPost.created_at && hasMore) {
                const createdAt =
                  lastPost.created_at instanceof Date
                    ? lastPost.created_at
                    : new Date(lastPost.created_at);
                if (!isNaN(createdAt.getTime())) {
                  nextCursor = `${createdAt.toISOString()}_${lastPost.id}`;
                }
              }

              console.log(
                `📊 [FEED SOURCE] User ${user_id}: ${trimmedCachedResponse.length} from redis (cached response)`
              );
              return res.status(200).json({
                success: true,
                feed: trimmedCachedResponse, // Use trimmed response
                count: trimmedCachedResponse.length, // Use trimmed count
                cached: true,
                has_more: hasMore,
                next_cursor: nextCursor,
              });
            }
          } else if (redisFeed && redisFeed.length === 0) {
            // Redis feed is empty but cache has posts - this is suspicious
            // It might mean posts weren't added to Redis feed
            console.log(
              `⚠️ [CACHE] Redis feed is EMPTY for user ${user_id} but cache has ${cachedResponse.length} posts. This might indicate fan-out didn't happen.`
            );
            // Still return cache since Redis is empty - but log the issue
            const hasMore = await feedService.checkHasMorePosts(
              user_id,
              limit,
              trimmedCachedResponse[trimmedCachedResponse.length - 1]
            );

            const lastPost =
              trimmedCachedResponse[trimmedCachedResponse.length - 1];
            let nextCursor = null;
            if (lastPost && lastPost.created_at && hasMore) {
              const createdAt =
                lastPost.created_at instanceof Date
                  ? lastPost.created_at
                  : new Date(lastPost.created_at);
              if (!isNaN(createdAt.getTime())) {
                nextCursor = `${createdAt.toISOString()}_${lastPost.id}`;
              }
            }

            console.log(
              `📊 [FEED SOURCE] User ${user_id}: ${trimmedCachedResponse.length} from redis (cached response, redis empty warning)`
            );
            return res.status(200).json({
              success: true,
              feed: trimmedCachedResponse, // Use trimmed response
              count: trimmedCachedResponse.length, // Use trimmed count
              cached: true,
              has_more: hasMore,
              next_cursor: nextCursor,
              warning:
                "Redis feed is empty - posts may not have been added via fan-out",
            });
          } else {
            // No posts in cache or Redis - return cached response (trimmed)
            const hasMore = await feedService.checkHasMorePosts(
              user_id,
              limit,
              trimmedCachedResponse[trimmedCachedResponse.length - 1]
            );

            const lastPost =
              trimmedCachedResponse[trimmedCachedResponse.length - 1];
            let nextCursor = null;
            if (lastPost && lastPost.created_at && hasMore) {
              const createdAt =
                lastPost.created_at instanceof Date
                  ? lastPost.created_at
                  : new Date(lastPost.created_at);
              if (!isNaN(createdAt.getTime())) {
                nextCursor = `${createdAt.toISOString()}_${lastPost.id}`;
              }
            }

            console.log(
              `📊 [FEED SOURCE] User ${user_id}: ${trimmedCachedResponse.length} from redis (cached response)`
            );
            return res.status(200).json({
              success: true,
              feed: trimmedCachedResponse, // Use trimmed response
              count: trimmedCachedResponse.length, // Use trimmed count
              cached: true,
              has_more: hasMore,
              next_cursor: nextCursor,
            });
          }
        } catch (validationError) {
          // If validation fails, just use the cache (better than failing)
          console.error(
            `⚠️ Cache validation error for user ${user_id}:`,
            validationError.message
          );
          const hasMore = await feedService.checkHasMorePosts(
            user_id,
            limit,
            trimmedCachedResponse[trimmedCachedResponse.length - 1]
          );

          const lastPost =
            trimmedCachedResponse[trimmedCachedResponse.length - 1];
          let nextCursor = null;
          if (lastPost && lastPost.created_at && hasMore) {
            const createdAt =
              lastPost.created_at instanceof Date
                ? lastPost.created_at
                : new Date(lastPost.created_at);
            if (!isNaN(createdAt.getTime())) {
              nextCursor = `${createdAt.toISOString()}_${lastPost.id}`;
            }
          }

          console.log(
            `📊 [FEED SOURCE] User ${user_id}: ${trimmedCachedResponse.length} from redis (cached response, validation error fallback)`
          );
          return res.status(200).json({
            success: true,
            feed: trimmedCachedResponse, // Use trimmed response
            count: trimmedCachedResponse.length, // Use trimmed count
            cached: true,
            has_more: hasMore,
            next_cursor: nextCursor,
          });
        }
      }
    }

    // Cache miss or stale or cursor provided - use hybrid approach: Redis first 100, PostgreSQL for beyond
    // FIX: Query for limit + 1 to accurately determine has_more
    const feed = await feedService.getFeed(user_id, limit + 1, cursor);

    // Determine has_more and trim to limit
    const hasMore = feed.length > limit;
    const trimmedFeed = hasMore ? feed.slice(0, limit) : feed;

    // Note: Feed source logging is already done in feedService.getFeed()

    if (trimmedFeed.length === 0) {
      return res.status(200).json({
        success: true,
        feed: [],
        count: 0,
        message: "No posts in feed",
        has_more: false,
        next_cursor: null,
      });
    }

    // Cache the complete response for next time (only if no cursor - first page)
    if (!cursor) {
      const redisKey = `feed:user:${user_id}`;
      const FEED_TTL = 7 * 24 * 60 * 60; // 7 days in seconds
      await cacheFeedResponse(redisKey, trimmedFeed, FEED_TTL).catch((err) =>
        console.error("Error caching feed response:", err)
      );
    }

    // Calculate next cursor for pagination
    const lastPost = trimmedFeed[trimmedFeed.length - 1];
    let nextCursor = null;

    if (lastPost && lastPost.created_at && hasMore) {
      const createdAt =
        lastPost.created_at instanceof Date
          ? lastPost.created_at
          : new Date(lastPost.created_at);

      if (!isNaN(createdAt.getTime())) {
        nextCursor = `${createdAt.toISOString()}_${lastPost.id}`;
      }
    }

    res.status(200).json({
      success: true,
      feed: trimmedFeed,
      count: trimmedFeed.length,
      cached: false,
      has_more: hasMore,
      next_cursor: nextCursor,
    });
  } catch (error) {
    console.error("Error fetching user feed:", error);
    res.status(500).json({
      success: false,
      message: "Error fetching user feed",
      error: error.message,
    });
  }
};
