import { User, Follow } from "../models/index.js";
import * as userCacheService from "../services/userCacheService.js";
import {
  invalidateFeedCache,
  backfillFeedOnFollow,
  removePostsFromFeedOnUnfollow,
} from "../services/feedService.js";
import {
  publishUserFollowed,
  publishUserUnfollowed,
} from "../services/kafkaProducer.js";

/**
 * Create a new user
 * POST /api/users
 */
export const createUser = async (req, res) => {
  try {
    const { username, email, bio, avatar_url } = req.body;

    if (!username || !email) {
      return res.status(400).json({
        success: false,
        message: "Username and email are required",
      });
    }

    const existingUser = await User.findOne({
      where: {
        email: email,
      },
    });

    if (existingUser) {
      return res.status(409).json({
        success: false,
        message: "User with this email already exists",
      });
    }

    const existingUsername = await User.findOne({
      where: {
        username: username,
      },
    });

    if (existingUsername) {
      return res.status(409).json({
        success: false,
        message: "Username is already taken",
      });
    }

    const newUser = await User.create({
      username,
      email,
      bio: bio || null,
      avatar_url: avatar_url || null,
      followers_count: 0,
      following_count: 0,
      is_celebrity: false,
    });

    res.status(201).json({
      newUser,
    });
  } catch (error) {
    console.error("Error creating user:", error);
    res.status(500).json({
      success: false,
      message: "Error creating user",
      error: error.message,
    });
  }
};

/**
 * Get all users
 * GET /api/users
 */
export const getAllUsers = async (req, res) => {
  try {
    const users = await User.findAll({
      attributes: [
        "id",
        "username",
        "email",
        "bio",
        "avatar_url",
        "followers_count",
        "following_count",
        "is_celebrity",
      ],
      order: [["id", "ASC"]],
    });

    res.status(200).json({
      users,
    });
  } catch (error) {
    console.error("Error fetching users:", error);
    res.status(500).json({
      success: false,
      message: "Error fetching users",
      error: error.message,
    });
  }
};

/**
 * Get user by ID
 * GET /api/users/:id
 */
export const getUserById = async (req, res) => {
  try {
    const { id } = req.params;

    if (isNaN(id)) {
      return res.status(400).json({
        success: false,
        message: "Invalid user ID",
      });
    }

    const user = await User.findByPk(id, {
      attributes: [
        "id",
        "username",
        "email",
        "bio",
        "avatar_url",
        "followers_count",
        "following_count",
        "is_celebrity",
      ],
    });

    if (!user) {
      return res.status(404).json({
        success: false,
        message: "User not found",
      });
    }

    res.status(200).json({
      user,
    });
  } catch (error) {
    console.error("Error fetching user:", error);
    res.status(500).json({
      success: false,
      message: "Error fetching user",
      error: error.message,
    });
  }
};

/**
 * Follow a user
 * POST /api/users/:id/follow
 * Body: { follower_id: <user_id> }
 */
export const followUser = async (req, res) => {
  try {
    const { id } = req.params;
    const { follower_id } = req.body;

    // Fast validation
    if (!follower_id) {
      return res.status(400).json({
        success: false,
        message: "follower_id is required in request body",
      });
    }

    const followingId = parseInt(id);
    const followerId = parseInt(follower_id);

    if (isNaN(followingId) || isNaN(followerId)) {
      return res.status(400).json({
        success: false,
        message: "Invalid user ID",
      });
    }

    if (followingId === followerId) {
      return res.status(400).json({
        success: false,
        message: "You cannot follow yourself",
      });
    }

    // OPTIMIZATION 1: Parallel user lookups + follow check
    const [userToFollow, follower, existingFollow] = await Promise.all([
      User.findByPk(followingId, {
        attributes: ["id", "username", "followers_count", "is_celebrity"],
      }),
      User.findByPk(followerId, {
        attributes: ["id", "username", "following_count"],
      }),
      Follow.findOne({
        where: {
          follower_id: followerId,
          following_id: followingId,
        },
      }),
    ]);

    if (!userToFollow) {
      return res.status(404).json({
        success: false,
        message: "User to follow not found",
      });
    }

    if (!follower) {
      return res.status(404).json({
        success: false,
        message: "Follower user not found",
      });
    }

    if (existingFollow) {
      return res.status(409).json({
        success: false,
        message: "You are already following this user",
      });
    }

    // OPTIMIZATION 2: Create follow + update counts in parallel
    const [follow] = await Promise.all([
      Follow.create({
        follower_id: followerId,
        following_id: followingId,
      }),
      // Update counts in database (parallel)
      User.increment("following_count", { where: { id: followerId } }),
      User.increment("followers_count", { where: { id: followingId } }),
      // Update counts in Redis cache (parallel, non-blocking)
      userCacheService.incrementFollowingCount(followerId).catch(() => {}),
      userCacheService.incrementFollowersCount(followingId).catch(() => {}),
    ]);

    // OPTIMIZATION 3: Calculate new counts directly (no reload needed)
    const newFollowingCount = follower.following_count + 1;
    const newFollowersCount = userToFollow.followers_count + 1;

    // OPTIMIZATION 4: Check celebrity status without reload
    const shouldUpdateCelebrity =
      newFollowersCount >= 10000 && !userToFollow.is_celebrity;
    if (shouldUpdateCelebrity) {
      User.update({ is_celebrity: true }, { where: { id: followingId } }).catch(
        console.error
      );
    }

    // Publish follow event to Kafka (non-blocking)
    setImmediate(async () => {
      try {
        await publishUserFollowed(followerId, followingId);
      } catch (kafkaError) {
        console.error(
          "⚠️ Error publishing follow event to Kafka:",
          kafkaError.message
        );
      }
    });

    // Backfill feed asynchronously (non-blocking)
    setImmediate(async () => {
      try {
        await backfillFeedOnFollow(followerId, followingId);
      } catch (backfillError) {
        console.error("⚠️ Error backfilling feed on follow:", backfillError);
        await invalidateFeedCache(followerId).catch(() => {});
      }
    });

    // Return immediately with calculated counts
    res.status(201).json({
      success: true,
      message: `User ${follower.username} is now following ${userToFollow.username}`,
      data: {
        follow,
        follower: {
          id: follower.id,
          username: follower.username,
          following_count: newFollowingCount,
        },
        following: {
          id: userToFollow.id,
          username: userToFollow.username,
          followers_count: newFollowersCount,
        },
      },
    });
  } catch (error) {
    console.error("Error following user:", error);
    res.status(500).json({
      success: false,
      message: "Error following user",
      error: error.message,
    });
  }
};

/**
 * Unfollow a user
 * POST /api/users/:id/unfollow
 * Body: { follower_id: <user_id> }
 */
export const unfollowUser = async (req, res) => {
  try {
    const { id } = req.params;
    const { follower_id } = req.body;

    if (!follower_id) {
      return res.status(400).json({
        success: false,
        message: "follower_id is required in request body",
      });
    }

    const followingId = parseInt(id);
    const followerId = parseInt(follower_id);

    if (isNaN(followingId) || isNaN(followerId)) {
      return res.status(400).json({
        success: false,
        message: "Invalid user ID",
      });
    }

    // OPTIMIZATION 1: Parallel user lookups + follow check
    const [userToUnfollow, follower, existingFollow] = await Promise.all([
      User.findByPk(followingId, {
        attributes: ["id", "username", "followers_count", "is_celebrity"],
      }),
      User.findByPk(followerId, {
        attributes: ["id", "username", "following_count"],
      }),
      Follow.findOne({
        where: {
          follower_id: followerId,
          following_id: followingId,
        },
      }),
    ]);

    if (!userToUnfollow) {
      return res.status(404).json({
        success: false,
        message: "User to unfollow not found",
      });
    }

    if (!follower) {
      return res.status(404).json({
        success: false,
        message: "Follower user not found",
      });
    }

    if (!existingFollow) {
      return res.status(404).json({
        success: false,
        message: "You are not following this user",
      });
    }

    // OPTIMIZATION 2: Delete follow + update counts in parallel
    await Promise.all([
      existingFollow.destroy(),
      // Update counts in parallel
      User.decrement("following_count", { where: { id: followerId } }),
      User.decrement("followers_count", { where: { id: followingId } }),
      // Update counts in Redis cache (parallel, non-blocking)
      userCacheService.decrementFollowingCount(followerId).catch(() => {}),
      userCacheService.decrementFollowersCount(followingId).catch(() => {}),
    ]);

    // OPTIMIZATION 3: Calculate new counts directly (no reload needed)
    const newFollowingCount = Math.max(0, follower.following_count - 1);
    const newFollowersCount = Math.max(0, userToUnfollow.followers_count - 1);

    // OPTIMIZATION 4: Check celebrity status without reload
    const shouldUpdateCelebrity =
      newFollowersCount < 10000 && userToUnfollow.is_celebrity;
    if (shouldUpdateCelebrity) {
      User.update(
        { is_celebrity: false },
        { where: { id: followingId } }
      ).catch(console.error);
    }

    // Publish unfollow event to Kafka (non-blocking)
    setImmediate(async () => {
      try {
        await publishUserUnfollowed(followerId, followingId);
      } catch (kafkaError) {
        console.error(
          "⚠️ Error publishing unfollow event to Kafka:",
          kafkaError
        );
      }
    });

    // Remove posts from feed asynchronously (non-blocking)
    setImmediate(async () => {
      try {
        await removePostsFromFeedOnUnfollow(followerId, followingId);
      } catch (removeError) {
        console.error(
          "⚠️ Error removing posts from feed on unfollow:",
          removeError
        );
        await invalidateFeedCache(followerId).catch(() => {});
      }
    });

    // Return immediately with calculated counts
    res.status(200).json({
      success: true,
      message: `User ${follower.username} has unfollowed ${userToUnfollow.username}`,
      data: {
        follower: {
          id: follower.id,
          username: follower.username,
          following_count: newFollowingCount,
        },
        unfollowed: {
          id: userToUnfollow.id,
          username: userToUnfollow.username,
          followers_count: newFollowersCount,
        },
      },
    });
  } catch (error) {
    console.error("Error unfollowing user:", error);
    res.status(500).json({
      success: false,
      message: "Error unfollowing user",
      error: error.message,
    });
  }
};
