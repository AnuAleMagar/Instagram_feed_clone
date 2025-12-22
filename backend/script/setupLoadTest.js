import { sequelize } from "../config/db.js";
import { User, Follow } from "../models/index.js";
import dotenv from "dotenv";

dotenv.config();

/**
 * Setup follow relationships for load testing
 * Makes a specified number of users follow a target user
 */
const setupLoadTest = async () => {
  try {
    await sequelize.authenticate();
    console.log("✅ Database connection established.");

    // Configuration
    const TEST_USER_ID = 5; // The user who will post (needs followers)
    const NUM_FOLLOWERS = 100; // How many followers to create

    // Check if test user exists
    const testUser = await User.findByPk(TEST_USER_ID);
    if (!testUser) {
      console.error(
        `❌ User ${TEST_USER_ID} not found. Please create it first.`
      );
      process.exit(1);
    }

    console.log(
      `\n📊 Setting up ${NUM_FOLLOWERS} followers for user ${TEST_USER_ID}...`
    );
    console.log(`   Test user: ${testUser.username} (ID: ${testUser.id})`);

    // Get all users except the test user
    const allUsers = await User.findAll({
      where: {
        id: { [sequelize.Sequelize.Op.ne]: TEST_USER_ID },
      },
      attributes: ["id", "username"],
      limit: NUM_FOLLOWERS,
    });

    if (allUsers.length < NUM_FOLLOWERS) {
      console.error(
        `❌ Not enough users! Need ${NUM_FOLLOWERS} users, but only found ${allUsers.length}.\n` +
          `   Please run seedUsers.js first to create more users.`
      );
      process.exit(1);
    }

    console.log(`✅ Found ${allUsers.length} users to make followers.`);

    // Create follow relationships in batches
    const batchSize = 50;
    const followsToCreate = [];
    let created = 0;
    let skipped = 0;

    for (const user of allUsers) {
      // Check if follow relationship already exists
      const existingFollow = await Follow.findOne({
        where: {
          follower_id: user.id,
          following_id: TEST_USER_ID,
        },
      });

      if (!existingFollow) {
        followsToCreate.push({
          follower_id: user.id,
          following_id: TEST_USER_ID,
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

    // Update follower count for test user
    const actualFollowCount = await Follow.count({
      where: { following_id: TEST_USER_ID },
    });

    await User.update(
      { followers_count: actualFollowCount },
      { where: { id: TEST_USER_ID } }
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

      // Then increment for users who actually follow user 5
      const usersWhoFollowTestUser = await Follow.findAll({
        where: { following_id: TEST_USER_ID },
        attributes: ["follower_id"],
      });

      const actualFollowerIds = usersWhoFollowTestUser.map(
        (f) => f.follower_id
      );
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
      `   User ${TEST_USER_ID} (${testUser.username}) now has ${actualFollowCount} followers`
    );
    console.log(`   Created: ${created} new follow relationships`);
    console.log(`   Skipped: ${skipped} (already existed)`);
    console.log(`\n🎯 Ready for load testing!`);
  } catch (error) {
    console.error("❌ Error setting up load test:", error);
    throw error;
  } finally {
    await sequelize.close();
    console.log("\n✅ Database connection closed.");
  }
};

// Run the setup
setupLoadTest()
  .then(() => {
    console.log("\n🎉 Setup completed!");
    process.exit(0);
  })
  .catch((error) => {
    console.error("\n❌ Setup failed:", error);
    process.exit(1);
  });
