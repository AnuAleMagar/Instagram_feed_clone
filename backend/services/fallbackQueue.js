/**
 * Fallback Queue Service
 * Handles async fan-out operations when Kafka is unavailable
 * Uses Redis as a simple queue to avoid blocking API responses
 */

import { redisClient } from "../config/db.js";
import * as feedService from "./feedService.js";

const FALLBACK_QUEUE_KEY = "fallback:fanout:queue";
const PROCESSING_KEY = (taskId) => `fallback:processing:${taskId}`;
const MAX_PROCESSING_TIME = 300000; // 5 minutes

/**
 * Add a fan-out task to the fallback queue
 * @param {Object} taskData - { userId, postId, createdAt }
 * @returns {Promise<string>} Task ID
 */
export async function enqueueFanOutTask(taskData) {
  const taskId = `${taskData.userId}:${taskData.postId}:${Date.now()}`;

  // Fix: Ensure createdAt is a Date object before calling toISOString()
  const createdAt =
    taskData.createdAt instanceof Date
      ? taskData.createdAt
      : new Date(taskData.createdAt);

  const task = {
    id: taskId,
    ...taskData,
    createdAt: createdAt.toISOString(),
    enqueuedAt: new Date().toISOString(),
  };

  // Add to queue (left push for FIFO)
  await redisClient.lPush(FALLBACK_QUEUE_KEY, JSON.stringify(task));

  // Set expiration on queue key (7 days)
  await redisClient.expire(FALLBACK_QUEUE_KEY, 7 * 24 * 60 * 60);

  return taskId;
}

/**
 * Process a single fan-out task from the queue
 * @returns {Promise<boolean>} True if task was processed, false if queue is empty
 */
export async function processFanOutTask() {
  try {
    // Get task from queue (right pop for FIFO)
    const taskJson = await redisClient.rPop(FALLBACK_QUEUE_KEY);

    if (!taskJson) {
      return false; // Queue is empty
    }

    const task = JSON.parse(taskJson);
    const processingKey = PROCESSING_KEY(task.id);

    // Mark as processing (with timeout to handle crashes)
    await redisClient.setEx(processingKey, MAX_PROCESSING_TIME / 1000, "1");

    try {
      // Perform fan-out
      await feedService.fanOutToFollowers(
        task.userId,
        task.postId,
        new Date(task.createdAt)
      );

      // Remove processing marker
      await redisClient.del(processingKey);
      return true;
    } catch (error) {
      console.error(
        `❌ [FALLBACK QUEUE] Error processing task ${task.id}:`,
        error.message
      );

      // Remove processing marker
      await redisClient.del(processingKey);

      // Re-enqueue for retry (with backoff - only retry once)
      if (!task.retryCount || task.retryCount < 1) {
        task.retryCount = (task.retryCount || 0) + 1;
        await redisClient.lPush(FALLBACK_QUEUE_KEY, JSON.stringify(task));
      } else {
        console.error(
          `❌ [FALLBACK QUEUE] Task ${task.id} exceeded max retries, dropping`
        );
      }
      throw error;
    }
  } catch (error) {
    console.error(`❌ [FALLBACK QUEUE] Error processing queue:`, error.message);
    return false;
  }
}

/**
 * Start background worker to process fallback queue
 * This should be called once at application startup
 */
export function startFallbackWorker() {
  const processQueue = async () => {
    try {
      const processed = await processFanOutTask();
      if (processed) {
        // Process next task immediately if queue has more
        setImmediate(processQueue);
      } else {
        // Queue empty, wait before checking again
        // Fix: Ensure timeout is always positive
        const delay = 1000; // Check every second
        setTimeout(processQueue, delay);
      }
    } catch (error) {
      console.error(`❌ [FALLBACK QUEUE] Worker error:`, error.message);
      // Wait before retrying on error
      // Fix: Ensure timeout is always positive
      const delay = 5000; // Wait 5 seconds on error
      setTimeout(processQueue, delay);
    }
  };

  // Start processing
  processQueue();
  console.log("✅ [FALLBACK QUEUE] Background worker started");
}

/**
 * Get queue length
 * @returns {Promise<number>} Number of tasks in queue
 */
export async function getQueueLength() {
  try {
    return await redisClient.lLen(FALLBACK_QUEUE_KEY);
  } catch (error) {
    console.error(
      `❌ [FALLBACK QUEUE] Error getting queue length:`,
      error.message
    );
    return 0;
  }
}
