import { sequelize } from "../config/db.js";
import { User, Follow } from "../models/index.js";
import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";

// ✅ Fix: Load .env from backend directory BEFORE importing db.js
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.join(__dirname, "../.env") });

/**
 * Setup follow relationships - Make all users follow user_id 1 (celebrity)
 */
const setupFollowRelationships = async () => {
  try {
    await sequelize.authenticate();
    console.log("✅ Database connection established.");

    // Configuration
    const CELEBRITY_USER_ID = 1; // The celebrity user

    // ✅ Create or update user1
    let user1 = await User.findByPk(CELEBRITY_USER_ID);

    if (!user1) {
      // Create user 1 if it doesn't exist
      user1 = await User.create({
        username: "user1",
        email: "user1@example.com",
        bio: "The celebrity user",
        avatar_url: "https://randomuser.me/api/portraits/men/1.jpg",
        followers_count: 0,
        following_count: 0,
        is_celebrity: true,
      });
      console.log("✅ Created user1 as celebrity");
    } else {
      // ✅ Ensure user 1 is set as celebrity
      if (!user1.is_celebrity) {
        await user1.update({ is_celebrity: true });
        console.log("✅ Updated user1 to be celebrity");
      } else {
        console.log("✅ User1 already exists and is celebrity");
      }
    }

    console.log(
      `\n📊 Setting up all users to follow user ${CELEBRITY_USER_ID}...`
    );
    console.log(`   Celebrity user: ${user1.username} (ID: ${user1.id})`);

    // ✅ Get ALL users except user 1 (no limit)
    const allUsers = await User.findAll({
      where: {
        id: { [sequelize.Sequelize.Op.ne]: CELEBRITY_USER_ID },
      },
      attributes: ["id", "username"],
    });

    console.log(`✅ Found ${allUsers.length} users to make followers.`);

    if (allUsers.length === 0) {
      console.log(`\n⚠️ No users found to follow user ${CELEBRITY_USER_ID}.`);
      console.log(
        `   Please run 'node script/seedUsers.js' first to create users.`
      );
      await sequelize.close();
      process.exit(0);
    }

    // Create follow relationships in batches
    const batchSize = 100;
    const followsToCreate = [];
    let created = 0;
    let skipped = 0;

    for (const user of allUsers) {
      // Check if follow relationship already exists
      const existingFollow = await Follow.findOne({
        where: {
          follower_id: user.id,
          following_id: CELEBRITY_USER_ID,
        },
      });

      if (!existingFollow) {
        followsToCreate.push({
          follower_id: user.id,
          following_id: CELEBRITY_USER_ID,
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

    // ✅ Update follower count for user 1
    const actualFollowCount = await Follow.count({
      where: { following_id: CELEBRITY_USER_ID },
    });

    await User.update(
      { followers_count: actualFollowCount },
      { where: { id: CELEBRITY_USER_ID } }
    );

    // ✅ Update following count for all followers
    const usersWhoFollowCelebrity = await Follow.findAll({
      where: { following_id: CELEBRITY_USER_ID },
      attributes: ["follower_id"],
    });

    const followerIds = usersWhoFollowCelebrity.map((f) => f.follower_id);
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

      // Then increment for users who actually follow user 1
      await User.increment("following_count", {
        where: {
          id: {
            [sequelize.Sequelize.Op.in]: followerIds,
          },
        },
      });
    }

    console.log(`\n✅ Setup complete!`);
    console.log(
      `   User ${CELEBRITY_USER_ID} (${user1.username}) now has ${actualFollowCount} followers`
    );
    console.log(`   Created: ${created} new follow relationships`);
    console.log(`   Skipped: ${skipped} (already existed)`);
    console.log(
      `\n🎯 All ${allUsers.length} users now follow user ${CELEBRITY_USER_ID}!`
    );

    // Verification
    console.log(`\n📊 Verification:`);
    const totalUsers = await User.count();
    const usersFollowingCelebrity = await Follow.count({
      where: { following_id: CELEBRITY_USER_ID },
    });
    console.log(`   Total users in database: ${totalUsers}`);
    console.log(
      `   Users following user ${CELEBRITY_USER_ID}: ${usersFollowingCelebrity}`
    );
    console.log(`   Expected: ${totalUsers - 1} (all users except user 1)`);

    if (usersFollowingCelebrity === totalUsers - 1) {
      console.log(`   ✅ Perfect! All users follow user ${CELEBRITY_USER_ID}`);
    } else {
      console.log(
        `   ⚠️ Missing ${
          totalUsers - 1 - usersFollowingCelebrity
        } follow relationships`
      );
    }

    const updatedCelebrity = await User.findByPk(CELEBRITY_USER_ID);
    console.log(
      `   User ${CELEBRITY_USER_ID} is_celebrity: ${updatedCelebrity.is_celebrity}`
    );
    console.log(
      `   User ${CELEBRITY_USER_ID} followers_count: ${updatedCelebrity.followers_count}`
    );
  } catch (error) {
    console.error("❌ Error setting up follow relationships:", error);
    throw error;
  } finally {
    await sequelize.close();
    console.log("\n✅ Database connection closed.");
  }
};

// Run the setup
setupFollowRelationships()
  .then(() => {
    console.log("\n🎉 Setup completed!");
    process.exit(0);
  })
  .catch((error) => {
    console.error("\n❌ Setup failed:", error);
    process.exit(1);
  });
