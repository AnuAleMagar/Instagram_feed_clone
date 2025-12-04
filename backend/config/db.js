import { Sequelize } from "sequelize";
import { createClient } from "redis";
import dotenv from "dotenv";
import { DB_CONFIG } from "./constants.js";

dotenv.config({ quiet: true });

if (!process.env.DATABASE_URL) {
  throw new Error("DATABASE_URL is not set in environment variables");
}

const sequelize = new Sequelize(process.env.DATABASE_URL, {
  dialect: "postgres",
  logging: process.env.NODE_ENV === "development" ? console.log : false,
  dialectOptions: {
    ssl: {
      require: true,
      rejectUnauthorized: false,
    },
  },
  pool: {
    max: DB_CONFIG.POSTGRES.POOL_MAX,
    min: DB_CONFIG.POSTGRES.POOL_MIN,
    acquire: DB_CONFIG.POSTGRES.POOL_ACQUIRE,
    idle: DB_CONFIG.POSTGRES.POOL_IDLE,
  },
});

// Redis Client Setup
const redisClient = createClient({
  socket: {
    host: process.env.REDIS_HOST || "localhost",
    port: parseInt(process.env.REDIS_PORT || "6379"),
    reconnectStrategy: (retries) => {
      if (retries > DB_CONFIG.REDIS.MAX_RETRIES) {
        return new Error("Max Redis reconnection retries exceeded");
      }
      return DB_CONFIG.REDIS.RETRY_DELAY * retries;
    },
  },
  password: process.env.REDIS_PASSWORD || undefined,
});

// Redis connection event handlers
redisClient.on("error", (err) => {
  console.error("❌ Redis Client Error:", err);
});

// Redis event handlers - silent for cleaner logs
redisClient.on("reconnecting", () => {
  // Silent reconnection
});

const initializeRedis = async () => {
  try {
    await redisClient.connect();
    console.log("✅ Connected to Redis");
    return redisClient;
  } catch (error) {
    console.error("❌ Unable to connect to Redis:", error);
    throw error;
  }
};

const closeRedis = async () => {
  try {
    await redisClient.quit();
  } catch (error) {
    console.error("Error closing Redis:", error);
  }
};

export {
  sequelize,
  redisClient,
  initializeRedis,
  closeRedis,
};
