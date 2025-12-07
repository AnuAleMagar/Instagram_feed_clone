// Suppress KafkaJS warnings
process.env.KAFKAJS_NO_PARTITIONER_WARNING = "1";

import { Kafka, CompressionTypes } from "kafkajs";
import SnappyCodec from "kafkajs-snappy";
import dotenv from "dotenv";
import { createRequire } from "module";

dotenv.config({ quiet: true });

// Register Snappy compression codec for KafkaJS
// This allows KafkaJS to encode/decode Snappy-compressed messages
// Use createRequire to access CompressionCodecs in ESM context
const require = createRequire(import.meta.url);
const { CompressionCodecs } = require("kafkajs");

CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

/**
 * Kafka Configuration
 *
 * This file sets up the Kafka client connection.
 * Kafka uses a broker-based architecture where:
 * - Producers send messages to topics
 * - Consumers read messages from topics
 * - Brokers store and manage topics
 *
 * NOTE: This configuration works with existing Kafka Confluent 7.4.0+ setups.
 * Set KAFKA_BROKERS in your .env file to point to your existing Kafka instance.
 */

// Kafka broker connection string
// Format: host:port (can be comma-separated for multiple brokers)
// Example: KAFKA_BROKERS=localhost:9092 or KAFKA_BROKERS=broker1:9092,broker2:9092
const brokers = process.env.KAFKA_BROKERS?.split(",") || ["localhost:9092"];

// Create Kafka client instance
export const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || "instagram-feed-backend",
  brokers: brokers,
  // Connection retry configuration - optimized for faster startup
  retry: {
    initialRetryTime: 100, // Start with 100ms
    retries: 8, // Retry up to 8 times
    multiplier: 2, // Double the retry time each attempt
    maxRetryTime: 10000, // Reduced from 30s to 10s
  },
  // Request timeout - reduced for faster failure detection
  requestTimeout: 10000, // Reduced from 30s to 10s
  // Connection timeout
  connectionTimeout: 3000, // 3 seconds
});

/**
 * Topic Names
 *
 * Topics are like categories or channels in Kafka.
 * Each topic can have multiple partitions for parallel processing.
 *
 * Naming convention: use kebab-case with descriptive names
 */
export const TOPICS = {
  // Post-related events
  POST_CREATED: "post-created", // When a new post is created
  POST_LIKED: "post-liked", // When a post is liked
  POST_UNLIKED: "post-unliked", // When a post is unliked

  // User relationship events
  USER_FOLLOWED: "user-followed", // When user A follows user B
  USER_UNFOLLOWED: "user-unfollowed", // When user A unfollows user B

  // Feed update events (internal)
  FEED_UPDATE: "feed-update", // Internal topic for feed fan-out operations
};

/**
 * Consumer Groups
 *
 * Consumer groups allow multiple consumers to work together.
 * Each message in a partition is delivered to only ONE consumer in a group.
 * This enables load balancing and parallel processing.
 */
export const CONSUMER_GROUPS = {
  FEED_PROCESSOR: "feed-processor-group", // Processes feed updates
  NOTIFICATION_SERVICE: "notification-service-group", // Could be used for notifications
  ANALYTICS: "analytics-group", // Could be used for analytics
};

/**
 * Initialize Kafka Admin Client
 *
 * Admin client is used for topic management (create, list, delete topics)
 */
export const admin = kafka.admin();

// Track Kafka connection status
let isKafkaConnected = false;

/**
 * Check if Kafka is connected
 */
export function isKafkaAvailable() {
  return isKafkaConnected;
}

/**
 * Connect to Kafka and create topics if they don't exist
 */
export async function initializeKafka() {
  try {
    await admin.connect();
    isKafkaConnected = true;
    console.log("✅ Connected to Kafka");

    // Get list of existing topics
    const existingTopics = await admin.listTopics();

    // Create topics if they don't exist (including DLQ topics)
    const allTopics = [
      ...Object.values(TOPICS),
      // Dead-letter queue topics
      `${TOPICS.POST_CREATED}-dlq`,
      `${TOPICS.USER_FOLLOWED}-dlq`,
      `${TOPICS.USER_UNFOLLOWED}-dlq`,
    ];

    const topicsToCreate = allTopics.map((topicName) => ({
      topic: topicName,
      numPartitions: 3, // 3 partitions for parallel processing
      replicationFactor: 1, // 1 replica (single broker setup)
      configEntries: [
        {
          name: "retention.ms",
          // DLQ topics have longer retention for investigation
          value: topicName.includes("-dlq")
            ? "2592000000" // 30 days for DLQ
            : "604800000", // 7 days for regular topics
        },
        // Note: compression.type is set to "producer" by default
        // This means the broker will use whatever compression the producer sends
        // Snappy codec is now registered above, so producers can use Snappy compression
      ],
    }));

    // Filter out topics that already exist
    const newTopics = topicsToCreate.filter(
      (topic) => !existingTopics.includes(topic.topic)
    );

    if (newTopics.length > 0) {
      await admin.createTopics({
        topics: newTopics,
        waitForLeaders: true, // Wait for partition leaders to be elected
      });
    }
  } catch (error) {
    isKafkaConnected = false;
    const errorMessage =
      error.cause?.code === "ECONNREFUSED"
        ? `Connection refused - Kafka broker is not available at ${
            process.env.KAFKA_BROKERS || "localhost:9092"
          }. Please ensure Kafka is running.`
        : error.message;

    console.error("❌ Error initializing Kafka:", errorMessage);
    if (error.cause?.code === "ECONNREFUSED") {
      console.error("   💡 Tip: Start Kafka with: docker-compose up -d");
    }
    throw error;
  }
}

/**
 * Disconnect from Kafka
 */
export async function disconnectKafka() {
  try {
    if (isKafkaConnected) {
      await admin.disconnect();
      isKafkaConnected = false;
    }
  } catch (error) {
    console.error("❌ Error disconnecting from Kafka:", error);
    isKafkaConnected = false;
  }
}
