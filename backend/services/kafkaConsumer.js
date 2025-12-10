import {
  kafka,
  TOPICS,
  CONSUMER_GROUPS,
  isKafkaAvailable,
} from "../config/kafka.js";
import * as feedService from "./feedService.js";
import { KAFKA_CONFIG } from "../config/constants.js";
import { producer } from "./kafkaProducer.js";

/**
 * Kafka Consumer Service
 *
 * WHAT IS A CONSUMER?
 * ===================
 * A consumer is like a subscriber that reads messages from Kafka topics.
 * Consumers process events asynchronously, allowing your application to
 * handle heavy operations (like fan-out) without blocking the main request.
 *
 * HOW IT WORKS:
 * 1. Consumer subscribes to one or more topics
 * 2. Consumer reads messages from partitions
 * 3. Consumer processes the message (e.g., update feeds)
 * 4. Consumer commits the offset (marks message as processed)
 *
 * KEY CONCEPTS:
 * - Consumer Group: Multiple consumers working together (load balancing)
 * - Partition: Each partition is consumed by only ONE consumer in a group
 * - Offset: Position in partition (tracks what's been read)
 * - Commit: Saving the offset (marks message as processed)
 *
 * BENEFITS:
 * - Decoupling: Producers don't wait for consumers
 * - Scalability: Add more consumers to handle more load
 * - Reliability: Messages are persisted and can be replayed
 * - Ordering: Messages in same partition are processed in order
 */

// Create consumer instance for feed processing
const feedConsumer = kafka.consumer({
  groupId: CONSUMER_GROUPS.FEED_PROCESSOR,
  // Consumer configuration - optimized for faster startup
  sessionTimeout: 30000, // 30s for stability
  heartbeatInterval: 3000, // REDUCED: 3 seconds (faster heartbeats = faster detection)
  maxBytesPerPartition: 1048576, // 1MB per partition
  minBytes: 1,
  maxBytes: 10485760, // 10MB total
  maxWaitTimeInMs: 1000, // REDUCED: 1 second (faster polling)
  // FIX: Add rebalance timeout to speed up group join
  rebalanceTimeout: 10000, // 10 seconds (reduces rebalance time)
  retry: {
    retries: 8,
    initialRetryTime: 100,
    multiplier: 2,
    maxRetryTime: 10000,
  },
});

let isRunning = false;

// Retry tracking (in-memory for simplicity, could use Redis for distributed systems)
const retryCounts = new Map();
const MAX_RETRY_DELAY = KAFKA_CONFIG.MAX_RETRY_DELAY || 60000;

/**
 * Get retry count for a message
 */
function getRetryCount(messageKey) {
  return retryCounts.get(messageKey) || 0;
}

/**
 * Increment retry count for a message
 */
function incrementRetryCount(messageKey) {
  const count = getRetryCount(messageKey);
  retryCounts.set(messageKey, count + 1);
  return count + 1;
}

/**
 * Clear retry count for a message
 */
function clearRetryCount(messageKey) {
  retryCounts.delete(messageKey);
}

/**
 * Retry a function with exponential backoff
 */
async function retryWithBackoff(event, processFn, messageKey) {
  const retryCount = incrementRetryCount(messageKey);

  if (retryCount > KAFKA_CONFIG.MAX_RETRIES) {
    return false; // Max retries exceeded
  }

  // Calculate delay with exponential backoff
  let delay = Math.max(
    0,
    Math.min(
      KAFKA_CONFIG.INITIAL_RETRY_DELAY *
        Math.pow(KAFKA_CONFIG.RETRY_MULTIPLIER, retryCount - 1),
      MAX_RETRY_DELAY
    )
  );

  // Safety check: ensure delay is never negative or invalid
  if (!Number.isFinite(delay) || delay < 0 || delay > MAX_RETRY_DELAY) {
    console.error(
      `⚠️ [KAFKA] Invalid delay calculated: ${delay}, using default 1000ms`
    );
    delay = 1000;
  }

  // Fix: Ensure delay is a valid positive integer for setTimeout
  const safeDelay = Math.max(1, Math.floor(delay));

  // Wait before retrying
  await new Promise((resolve) => setTimeout(resolve, safeDelay));

  try {
    await processFn(event);
    clearRetryCount(messageKey);
    return true; // Success
  } catch (error) {
    console.error(
      `❌ [KAFKA] Retry attempt ${retryCount} failed:`,
      error.message
    );
    // If we've hit max retries, return false
    if (retryCount >= KAFKA_CONFIG.MAX_RETRIES) {
      return false;
    }
    // Otherwise, recursively retry
    return await retryWithBackoff(event, processFn, messageKey);
  }
}

/**
 * Send message to Dead-Letter Queue (DLQ)
 */
async function sendToDLQ(
  event,
  originalError,
  retryCount,
  originalTopic = TOPICS.POST_CREATED
) {
  try {
    const dlqTopic = `${originalTopic}${KAFKA_CONFIG.DLQ_TOPIC_SUFFIX}`;
    const dlqMessage = {
      originalEvent: event,
      originalError: {
        message: originalError.message,
        stack: originalError.stack,
      },
      retryCount,
      timestamp: new Date().toISOString(),
      reason: "Max retries exceeded",
    };

    await producer.send({
      topic: dlqTopic,
      messages: [
        {
          key: event.userId || event.postId || "unknown",
          value: JSON.stringify(dlqMessage),
          headers: {
            "content-type": "application/json",
            "x-original-topic": originalTopic,
            "x-retry-count": String(retryCount),
          },
        },
      ],
    });
  } catch (error) {
    console.error(`❌ [DLQ] Failed to send message to DLQ:`, error.message);
    // Don't throw - we don't want DLQ failures to crash the consumer
  }
}

/**
 * Get topic name from event type
 * @param {string} eventType - Event type string
 * @returns {string} Topic name
 */
function getTopicFromEventType(eventType) {
  switch (eventType) {
    case "POST_CREATED":
      return TOPICS.POST_CREATED;
    case "USER_FOLLOWED":
      return TOPICS.USER_FOLLOWED;
    case "USER_UNFOLLOWED":
      return TOPICS.USER_UNFOLLOWED;
    default:
      return TOPICS.POST_CREATED; // Default fallback
  }
}

/**
 * Initialize and start the feed consumer
 *
 * This consumer listens for POST_CREATED events and performs
 * the fan-out operation asynchronously.
 */
export async function startFeedConsumer() {
  if (isRunning) {
    return;
  }

  if (!isKafkaAvailable()) {
    return;
  }

  try {
    await feedConsumer.connect();

    // Subscribe to topics
    await feedConsumer.subscribe({
      topics: [
        TOPICS.POST_CREATED,
        TOPICS.USER_FOLLOWED,
        TOPICS.USER_UNFOLLOWED,
      ],
      fromBeginning: false,
    });

    // FIX: Start consuming in background (non-blocking)
    // Don't await - let it join the group in background
    feedConsumer.run({
      eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
          // Wrap entire batch in try-catch to prevent crashes
          try {
            for (const message of batch.messages) {
              const messageKey = `${message.partition}:${message.offset}`;

              try {
                // Parse the message value
                const event = JSON.parse(message.value.toString());

                // Determine topic from event type
                const eventTopic = getTopicFromEventType(event.eventType);

                // Process based on event type
                try {
                  await processEvent(event);
                } catch (processError) {
                  console.error(
                    `❌ [KAFKA] Error in processEvent:`,
                    processError.message
                  );
                  // Don't throw - let retry logic handle it
                  throw processError;
                }

                // Clear retry count on success
                clearRetryCount(messageKey);
                resolveOffset(message.offset);

                // Mark message as processed (commit offset) - ONLY on success
                await heartbeat();
              } catch (error) {
                console.error(
                  `❌ [KAFKA] Error processing message at offset ${message.offset}:`,
                  error.message
                );

                // Try to parse event for retry logic
                let event = null;
                let eventTopic = TOPICS.POST_CREATED; // Default
                try {
                  event = JSON.parse(message.value.toString());
                  eventTopic = getTopicFromEventType(event.eventType);
                } catch (parseError) {
                  console.error(
                    `❌ [KAFKA] Failed to parse message, sending to DLQ:`,
                    parseError.message
                  );
                  // Can't retry if we can't parse - send to DLQ
                  try {
                    await sendToDLQ(
                      { raw: message.value.toString() },
                      error,
                      getRetryCount(messageKey),
                      eventTopic
                    );
                  } catch (dlqError) {
                    console.error(
                      "❌ [KAFKA] DLQ send failed:",
                      dlqError.message
                    );
                    // Even if DLQ fails, commit to prevent infinite retries
                  }
                  resolveOffset(message.offset);
                  clearRetryCount(messageKey);
                  await heartbeat();
                  continue;
                }

                // Retry with exponential backoff
                try {
                  const success = await retryWithBackoff(
                    event,
                    processEvent,
                    messageKey
                  );

                  if (success) {
                    // Successfully processed after retry
                    resolveOffset(message.offset);
                  } else {
                    // Max retries exceeded - send to DLQ
                    console.error(
                      `📮 [DLQ] Sending message to dead-letter queue after ${KAFKA_CONFIG.MAX_RETRIES} failed retries`
                    );
                    try {
                      await sendToDLQ(
                        event,
                        error,
                        KAFKA_CONFIG.MAX_RETRIES,
                        eventTopic
                      );
                    } catch (dlqError) {
                      console.error(
                        "❌ [KAFKA] DLQ send failed:",
                        dlqError.message
                      );
                    }

                    // Only commit after sending to DLQ to prevent infinite retries
                    resolveOffset(message.offset);
                    clearRetryCount(messageKey);
                  }
                } catch (retryError) {
                  // If retry mechanism itself fails, commit and move on
                  console.error(
                    `❌ [KAFKA] Retry mechanism failed:`,
                    retryError.message
                  );
                  resolveOffset(message.offset);
                  clearRetryCount(messageKey);
                }

                // Send heartbeat to keep consumer alive
                await heartbeat();
              }
            }
          } catch (batchError) {
            // Catch any errors that escape the message loop
            console.error(
              `❌ [KAFKA] Fatal error in batch processing:`,
              batchError.message,
              batchError.stack
            );
            // Send heartbeat to keep consumer alive even on batch errors
            try {
              await heartbeat();
            } catch (heartbeatError) {
              console.error(
                `❌ [KAFKA] Heartbeat failed after batch error:`,
                heartbeatError.message
              );
            }
            // Don't rethrow - we want to continue processing
          }
        },
      })
      .catch((error) => {
        console.error("❌ [KAFKA] Consumer run error:", error);
        isRunning = false;
      });

    // Mark as running immediately (consumer will join group in background)
    isRunning = true;
    console.log("✅ Kafka consumer starting (joining group in background)...");

    // Wait a short time for initial connection, but don't block on full join
    // The consumer will be ready when it joins the group
    await new Promise((resolve) => setTimeout(resolve, 500)); // Give it 500ms to start

    // Add error handlers to prevent crashes
    feedConsumer.on("consumer.crash", ({ error }) => {
      console.error("❌ [KAFKA] Consumer crashed:", error);
      isRunning = false;
    });

    feedConsumer.on("consumer.disconnect", () => {
      console.warn("⚠️ [KAFKA] Consumer disconnected");
      isRunning = false;
    });

    feedConsumer.on("consumer.network.request_timeout", ({ payload }) => {
      console.warn("⚠️ [KAFKA] Network request timeout:", payload);
    });

    // Listen for successful group join
    feedConsumer.on("consumer.group_join", ({ payload }) => {
      console.log("✅ [KAFKA] Consumer has joined the group");
    });
  } catch (error) {
    isRunning = false;
    const errorMessage =
      error.cause?.code === "ECONNREFUSED"
        ? "Connection refused - Kafka broker is not available"
        : error.message;
    console.error("❌ Error starting feed consumer:", errorMessage);
    throw error;
  }
}

/**
 * Process an event from Kafka
 *
 * @param {Object} event - Event data
 */
async function processEvent(event) {
  const { eventType } = event;

  switch (eventType) {
    case "POST_CREATED":
      await handlePostCreated(event);
      break;

    case "USER_FOLLOWED":
      await handleUserFollowed(event);
      break;

    case "USER_UNFOLLOWED":
      await handleUserUnfollowed(event);
      break;

    default:
      console.warn(`⚠️ [KAFKA] Unknown event type: ${eventType}`);
  }
}

/**
 * Handle POST_CREATED event
 *
 * When a post is created, we need to:
 * 1. Get all followers of the post creator
 * 2. Add the post to each follower's feed (fan-out)
 *
 * This is done asynchronously via Kafka, so the API response
 * doesn't wait for the fan-out to complete.
 */
async function handlePostCreated(event) {
  const { postId, userId, createdAt } = event;

  if (!postId || !userId || !createdAt) {
    throw new Error(
      `Invalid POST_CREATED event: missing required fields. postId: ${postId}, userId: ${userId}, createdAt: ${createdAt}`
    );
  }

  // Perform fan-out to followers' feeds
  // Errors will be caught by the retry mechanism above
  await feedService.fanOutToFollowers(userId, postId, new Date(createdAt));
}

/**
 * Handle USER_FOLLOWED event
 */
async function handleUserFollowed(event) {
  const { followerId, followingId } = event;

  if (!followerId || !followingId) {
    throw new Error(
      `Invalid USER_FOLLOWED event: missing required fields. followerId: ${followerId}, followingId: ${followingId}`
    );
  }

  // Backfill feed is already handled synchronously in userController
  // This handler can be used for additional async processing if needed
  // Additional async processing can be added here if needed
}

/**
 * Handle USER_UNFOLLOWED event
 */
async function handleUserUnfollowed(event) {
  const { followerId, followingId } = event;

  if (!followerId || !followingId) {
    throw new Error(
      `Invalid USER_UNFOLLOWED event: missing required fields. followerId: ${followerId}, followingId: ${followingId}`
    );
  }

  // Post removal is already handled synchronously in userController
  // This handler can be used for additional async processing if needed
  // Additional async processing can be added here if needed
}

/**
 * Stop the feed consumer
 */
export async function stopFeedConsumer() {
  if (!isRunning) {
    return;
  }

  try {
    await feedConsumer.disconnect();
    isRunning = false;
    console.log("✅ Kafka consumer stopped");
  } catch (error) {
    console.error("❌ Error stopping feed consumer:", error);
    isRunning = false; // Mark as stopped even on error
    // Don't throw - graceful shutdown
  }
}

// Export consumer instance for advanced usage
export { feedConsumer };
