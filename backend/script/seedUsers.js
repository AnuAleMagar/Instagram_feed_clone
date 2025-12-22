import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.join(__dirname, "../.env") });

const FIRST_NAMES = [
  "james", "olivia", "liam", "emma", "noah", "ava", "william", "sophia",
  "benjamin", "isabella", "lucas", "mia", "henry", "amelia", "alexander",
  "harper", "mason", "evelyn", "ethan", "abigail", "daniel", "ella",
  "matthew", "scarlett", "aiden", "grace", "jackson", "chloe", "sebastian",
  "victoria", "jack", "riley", "owen", "aria", "samuel", "lily", "ryan",
  "aubrey", "nathan", "zoey", "adam", "nora", "isaac", "hannah", "caleb",
  "lillian", "julian", "addison", "elijah", "stella", "leo", "natalie",
  "gabriel", "zoe", "anthony", "leah", "dylan", "hazel", "evan", "violet",
  "aaron", "aurora", "charles", "savannah", "thomas", "audrey", "eli",
  "brooklyn", "christian", "bella", "connor", "claire", "josiah", "skylar",
  "andrew", "lucy", "john", "paisley", "lincoln", "everly", "david",
  "anna", "hunter", "caroline", "joshua", "genesis", "christopher", "aaliyah",
  "grayson", "kennedy", "michael", "kinsley", "jayden", "maya", "carter",
];

const LAST_NAMES = [
  "smith", "johnson", "williams", "brown", "jones", "garcia", "miller",
  "davis", "rodriguez", "martinez", "hernandez", "lopez", "gonzalez",
  "wilson", "anderson", "thomas", "taylor", "moore", "jackson", "martin",
  "lee", "perez", "thompson", "white", "harris", "sanchez", "clark",
  "ramirez", "lewis", "robinson", "walker", "young", "allen", "king",
  "wright", "scott", "torres", "nguyen", "hill", "flores", "green",
  "adams", "nelson", "baker", "hall", "rivera", "campbell", "mitchell",
  "carter", "roberts", "turner", "phillips", "evans", "parker", "edwards",
  "collins", "stewart", "morris", "rogers", "reed", "cook", "morgan",
  "bell", "murphy", "bailey", "cooper", "richardson", "cox", "howard",
  "ward", "torres", "peterson", "gray", "ramirez", "james", "watson",
  "brooks", "kelly", "sanders", "price", "bennett", "wood", "barnes",
  "ross", "henderson", "coleman", "jenkins", "perry", "powell", "long",
  "patterson", "hughes", "flores", "washington", "butler", "simmons",
];

function randomFrom(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function generateUsername(index) {
  const first = randomFrom(FIRST_NAMES);
  const last = randomFrom(LAST_NAMES);
  return `${first}_${last}${index}`;
}

const NUM_USERS = 500;
const BATCH_SIZE = 100;

const seed = async () => {
  const { sequelize } = await import("../config/db.js");
  const { User, Follow } = await import("../models/index.js");

  try {
    await sequelize.authenticate();
    console.log("Connected to database");

    // Create user1 as the celebrity first
    const [celebrity] = await User.findOrCreate({
      where: { username: "user1" },
      defaults: {
        email: "user1@example.com",
        bio: null,
        avatar_url: null,
        followers_count: 0,
        following_count: 0,
        is_celebrity: true,
      },
    });

    // Mark as celebrity in case they already existed
    await User.update({ is_celebrity: true }, { where: { id: celebrity.id } });
    console.log(`Celebrity: user1 (id: ${celebrity.id})`);

    // Build 499 random users (user1 already created above)
    const users = [];
    for (let i = 2; i <= NUM_USERS; i++) {
      const username = generateUsername(i);
      users.push({
        username,
        email: `${username}@example.com`,
        bio: null,
        avatar_url: null,
        followers_count: 0,
        following_count: 0,
        is_celebrity: false,
      });
    }

    // Bulk insert in batches, skip duplicates
    console.log(`\nInserting ${users.length} random users in batches of ${BATCH_SIZE}...`);
    let totalCreated = 0;

    for (let i = 0; i < users.length; i += BATCH_SIZE) {
      const batch = users.slice(i, i + BATCH_SIZE);
      const result = await User.bulkCreate(batch, { ignoreDuplicates: true });
      totalCreated += result.length;
      console.log(`  Batch ${Math.floor(i / BATCH_SIZE) + 1}: inserted ${result.length} users (total: ${totalCreated})`);
    }

    console.log(`\nUsers created: ${totalCreated + 1} (including user1)`);

    // Get all other users
    const { Op } = (await import("sequelize")).default;
    const followers = await User.findAll({
      where: { id: { [Op.ne]: celebrity.id } },
      attributes: ["id"],
    });

    console.log(`\nCreating ${followers.length} follow relationships...`);

    const follows = followers.map((u) => ({
      follower_id: u.id,
      following_id: celebrity.id,
      created_at: new Date(),
    }));

    for (let i = 0; i < follows.length; i += BATCH_SIZE) {
      const batch = follows.slice(i, i + BATCH_SIZE);
      await Follow.bulkCreate(batch, { ignoreDuplicates: true });
      console.log(`  Follow batch ${Math.floor(i / BATCH_SIZE) + 1} done`);
    }

    // Sync follower/following counts
    const followerCount = await Follow.count({ where: { following_id: celebrity.id } });
    await User.update({ followers_count: followerCount }, { where: { id: celebrity.id } });
    await User.update(
      { following_count: 1 },
      { where: { id: { [Op.in]: followers.map((u) => u.id) } } }
    );

    console.log(`\nDone.`);
    console.log(`  ${celebrity.username} has ${followerCount} followers`);
  } finally {
    await sequelize.close();
  }
};

seed().catch((err) => {
  console.error(err);
  process.exit(1);
});
