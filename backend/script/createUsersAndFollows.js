import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";

// ✅ Fix: Load .env from backend directory BEFORE importing db.js
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const envPath = path.join(__dirname, "../.env");
dotenv.config({ path: envPath });

// Debug: Log connection parameters
console.log("🔍 Database connection parameters:");
console.log(`   DB_HOST: ${process.env.DB_HOST || "localhost (default)"}`);
console.log(`   DB_PORT: ${process.env.DB_PORT || "5432 (default)"}`);
console.log(`   DB_NAME: ${process.env.DB_NAME || "demo (default)"}`);
console.log(`   DB_USER: ${process.env.DB_USER || "postgres (default)"}`);
console.log(`   .env file loaded from: ${envPath}\n`);

/**
 * Creates users from user1 to user500 and makes all users follow user1
 */
const createUsersAndFollows = async () => {
  // Use dynamic import to ensure env vars are loaded first
  const { sequelize } = await import("../config/db.js");
  const { User, Follow } = await import("../models/index.js");
  try {
    await sequelize.authenticate();
    console.log("✅ Database connection established.");

    const NUM_USERS = 500;
    const TARGET_USERNAME = "user1"; // The user everyone will follow

    console.log(`\n📊 Creating ${NUM_USERS} users...`);

    // Check if user1 already exists
    let targetUser = await User.findOne({
      where: { username: TARGET_USERNAME },
    });

    if (!targetUser) {
      // Create user1 first
      targetUser = await User.create({
        username: TARGET_USERNAME,
        email: `${TARGET_USERNAME}@example.com`,
        bio: `User ${TARGET_USERNAME}`,
        followers_count: 0,
        following_count: 0,
        is_celebrity: false,
      });
      console.log(`✅ Created ${TARGET_USERNAME} (ID: ${targetUser.id})`);
    } else {
      console.log(
        `ℹ️  ${TARGET_USERNAME} already exists (ID: ${targetUser.id})`
      );
    }

    const TARGET_USER_ID = targetUser.id;

    // Create users user2 to user500
    const usersToCreate = [];
    const batchSize = 50;

    for (let i = 2; i <= NUM_USERS; i++) {
      const username = `user${i}`;
      const email = `user${i}@example.com`;

      // Check if user already exists
      const existingUser = await User.findOne({
        where: { username },
      });

      if (!existingUser) {
        usersToCreate.push({
          username,
          email,
          bio: `User ${username}`,
          followers_count: 0,
          following_count: 0,
          is_celebrity: false,
        });
      }

      // Create users in batches
      if (usersToCreate.length >= batchSize) {
        try {
          const created = await User.bulkCreate(usersToCreate, {
            ignoreDuplicates: true,
          });
          console.log(
            `✅ Created ${created.length} users (Total: ${i - 1}/${
              NUM_USERS - 1
            })`
          );
          usersToCreate.length = 0;
        } catch (error) {
          console.error(`⚠️ Error creating user batch:`, error.message);
          usersToCreate.length = 0;
        }
      }
    }

    // Create remaining users
    if (usersToCreate.length > 0) {
      try {
        const created = await User.bulkCreate(usersToCreate, {
          ignoreDuplicates: true,
        });
        console.log(`✅ Created ${created.length} remaining users`);
      } catch (error) {
        console.error(`⚠️ Error creating final user batch:`, error.message);
      }
    }

    console.log(`\n📊 Setting up follow relationships...`);
    console.log(
      `   All users will follow ${TARGET_USERNAME} (ID: ${TARGET_USER_ID})`
    );

    // Get all users except user1
    const allUsers = await User.findAll({
      where: {
        id: { [sequelize.Sequelize.Op.ne]: TARGET_USER_ID },
      },
      attributes: ["id", "username"],
    });

    console.log(`✅ Found ${allUsers.length} users to make followers.`);

    // Create follow relationships in batches
    const followsToCreate = [];
    let created = 0;
    let skipped = 0;

    for (const user of allUsers) {
      // Check if follow relationship already exists
      const existingFollow = await Follow.findOne({
        where: {
          follower_id: user.id,
          following_id: TARGET_USER_ID,
        },
      });

      if (!existingFollow) {
        followsToCreate.push({
          follower_id: user.id,
          following_id: TARGET_USER_ID,
          created_at: new Date(),
        });
      } else {
        skipped++;
      }

      // Process in batches
      if (followsToCreate.length >= batchSize) {
        try {
          await Follow.bulkCreate(followsToCreate, {
            ignoreDuplicates: true,
          });
          created += followsToCreate.length;
          console.log(
            `✅ Created ${followsToCreate.length} follow relationships (Total: ${created})`
          );
          followsToCreate.length = 0;
        } catch (error) {
          console.error(`⚠️ Error creating follow batch:`, error.message);
          followsToCreate.length = 0;
        }
      }
    }

    // Create remaining follows
    if (followsToCreate.length > 0) {
      try {
        await Follow.bulkCreate(followsToCreate, {
          ignoreDuplicates: true,
        });
        created += followsToCreate.length;
        console.log(
          `✅ Created ${followsToCreate.length} follow relationships (Total: ${created})`
        );
      } catch (error) {
        console.error(`⚠️ Error creating final follow batch:`, error.message);
      }
    }

    // Update follower count for user1
    const actualFollowCount = await Follow.count({
      where: { following_id: TARGET_USER_ID },
    });

    await User.update(
      { followers_count: actualFollowCount },
      { where: { id: TARGET_USER_ID } }
    );

    // Update following count for all followers
    const followerIds = allUsers.map((u) => u.id);
    if (followerIds.length > 0) {
      // Reset following_count to 0 first for these users
      await User.update(
        { following_count: 0 },
        {
          where: {
            id: {
              [sequelize.Sequelize.Op.in]: followerIds,
            },
          },
        }
      );

      // Then increment for users who actually follow user1
      const usersWhoFollowTarget = await Follow.findAll({
        where: { following_id: TARGET_USER_ID },
        attributes: ["follower_id"],
      });

      const actualFollowerIds = usersWhoFollowTarget.map((f) => f.follower_id);
      if (actualFollowerIds.length > 0) {
        await User.increment("following_count", {
          where: {
            id: {
              [sequelize.Sequelize.Op.in]: actualFollowerIds,
            },
          },
        });
      }
    }

    console.log(`\n✅ Setup complete!`);
    console.log(
      `   ${TARGET_USERNAME} (ID: ${TARGET_USER_ID}) now has ${actualFollowCount} followers`
    );
    console.log(`   Created: ${created} new follow relationships`);
    console.log(`   Skipped: ${skipped} (already existed)`);
    console.log(`\n🎯 All users (user1 to user${NUM_USERS}) are ready!`);
  } catch (error) {
    console.error("❌ Error creating users and follows:", error);
    throw error;
  } finally {
    await sequelize.close();
    console.log("\n✅ Database connection closed.");
  }
};

// Run the script
createUsersAndFollows()
  .then(() => {
    console.log("\n🎉 Script completed successfully!");
    process.exit(0);
  })
  .catch((error) => {
    console.error("\n❌ Script failed:", error);
    process.exit(1);
  });
