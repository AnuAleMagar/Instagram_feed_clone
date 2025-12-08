import { kafka, TOPICS, isKafkaAvailable } from "../config/kafka.js";

/**
 * Kafka Producer Service
 *
 * WHAT IS A PRODUCER?
 * ===================
 * A producer is like a publisher in a messaging system. It sends messages (events)
 * to Kafka topics. Think of it as a broadcaster that announces events to anyone
 * who's listening (consumers).
 *
 * HOW IT WORKS:
 * 1. Producer creates a message with a key and value
 * 2. Kafka determines which partition to send the message to (based on key)
 * 3. Message is appended to the partition's log
 * 4. Consumers can then read from the partition
 *
 * KEY CONCEPTS:
 * - Key: Used for partitioning (messages with same key go to same partition)
 * - Value: The actual message/event data
 * - Partition: A topic is divided into partitions for parallel processing
 * - Offset: Each message has a unique position (offset) in a partition
 */

// Create producer instance
const producer = kafka.producer({
  // Producer configuration
  maxInFlightRequests: 1, // Process one request at a time (ensures ordering)
  idempotent: true, // Prevent duplicate messages
  transactionTimeout: 30000, // 30 seconds
  retry: {
    retries: 10, // Increased from 3 for better reliability
    initialRetryTime: 100,
    multiplier: 2,
    maxRetryTime: 30000,
  },
});

let isConnected = false;

/**
 * Connect the producer to Kafka
 */
export async function connectProducer() {
  if (isConnected) {
    return;
  }

  if (!isKafkaAvailable()) {
    return;
  }

  try {
    await producer.connect();
    isConnected = true;
    console.log("✅ Kafka producer connected");
  } catch (error) {
    isConnected = false;
    const errorMessage =
      error.cause?.code === "ECONNREFUSED"
        ? "Connection refused - Kafka broker is not available"
        : error.message;
    console.error("❌ Error connecting Kafka producer:", errorMessage);
    throw error;
  }
}

/**
 * Disconnect the producer
 */
export async function disconnectProducer() {
  if (!isConnected) {
    return;
  }

  try {
    await producer.disconnect();
    isConnected = false;
  } catch (error) {
    console.error("❌ Error disconnecting Kafka producer:", error);
  }
}

/**
 * Send a message to a Kafka topic
 *
 * @param {string} topic - Topic name
 * @param {Object} message - Message payload
 * @param {string} key - Optional message key (for partitioning)
 * @param {Object} headers - Optional message headers
 */
export async function sendMessage(topic, message, key = null, headers = {}) {
  // Check if Kafka is available
  if (!isKafkaAvailable()) {
    return {
      success: false,
      error: "Kafka is not available",
      message: "Message not sent - Kafka broker is unavailable",
    };
  }

  if (!isConnected) {
    try {
      await connectProducer();
    } catch (error) {
      // Don't throw - return error object instead
      return {
        success: false,
        error: "Producer connection failed",
        message: error.message,
      };
    }
  }

  try {
    const messagePayload = {
      topic: topic,
      messages: [
        {
          key: key ? String(key) : null,
          value: JSON.stringify(message),
          headers: {
            ...headers,
            "content-type": "application/json",
            timestamp: new Date().toISOString(),
          },
        },
      ],
    };

    const result = await producer.send(messagePayload);

    return {
      success: true,
      partition: result[0]?.partition,
      offset: result[0]?.offset,
    };
  } catch (error) {
    isConnected = false; // Mark as disconnected on error
    const errorMessage =
      error.cause?.code === "ECONNREFUSED"
        ? "Connection refused - Kafka broker is not available"
        : error.message;
    console.error(
      `❌ [KAFKA] Error sending message to topic "${topic}":`,
      errorMessage
    );
    // NEVER throw - always return error object
    return {
      success: false,
      error: errorMessage,
      message: "Message not sent due to Kafka error",
    };
  }
}

/**
 * Publish a post created event
 *
 * This is called when a new post is created. The event will be consumed
 * by feed processors to update followers' feeds asynchronously.
 *
 * @param {Object} postData - Post data
 * @param {string} postData.id - Post ID
 * @param {number} postData.user_id - User ID who created the post
 * @param {Date} postData.created_at - Post creation timestamp
 */
export async function publishPostCreated(postData) {
  // Fix: Ensure created_at is a Date object before calling toISOString()
  const createdAt =
    postData.created_at instanceof Date
      ? postData.created_at
      : new Date(postData.created_at);

  const event = {
    eventType: "POST_CREATED",
    postId: postData.id,
    userId: postData.user_id,
    createdAt: createdAt.toISOString(),
    timestamp: new Date().toISOString(),
  };

  // Use user_id as key to ensure all posts from same user go to same partition
  // This maintains ordering of posts from the same user
  return await sendMessage(TOPICS.POST_CREATED, event, postData.user_id);
}

/**
 * Publish a user followed event
 *
 * This is called when user A follows user B. This event can be used to:
 * - Backfill user A's feed with user B's recent posts
 * - Update analytics
 * - Send notifications
 *
 * @param {number} followerId - User who is following
 * @param {number} followingId - User being followed
 */
export async function publishUserFollowed(followerId, followingId) {
  const event = {
    eventType: "USER_FOLLOWED",
    followerId,
    followingId,
    timestamp: new Date().toISOString(),
  };

  // Use followerId as key to maintain ordering per follower
  return await sendMessage(TOPICS.USER_FOLLOWED, event, followerId);
}

/**
 * Publish a user unfollowed event
 *
 * This is called when user A unfollows user B. This event can be used to:
 * - Remove user B's posts from user A's feed
 * - Update analytics
 *
 * @param {number} followerId - User who is unfollowing
 * @param {number} followingId - User being unfollowed
 */
export async function publishUserUnfollowed(followerId, followingId) {
  const event = {
    eventType: "USER_UNFOLLOWED",
    followerId,
    followingId,
    timestamp: new Date().toISOString(),
  };

  return await sendMessage(TOPICS.USER_UNFOLLOWED, event, followerId);
}

// Export producer instance for advanced usage
export { producer };
