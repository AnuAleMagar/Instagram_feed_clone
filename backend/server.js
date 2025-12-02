import express from "express";
import dotenv from "dotenv";

import {
  sequelize,
  initializeRedis,
  redisClient,
} from "./config/db.js";
import { loadLuaScripts } from "./services/redisLuaScripts.js";
import { initializeKafka, disconnectKafka } from "./config/kafka.js";
import {
  connectProducer,
  disconnectProducer,
} from "./services/kafkaProducer.js";
import {
  startFeedConsumer,
  stopFeedConsumer,
} from "./services/kafkaConsumer.js";
import { startFallbackWorker } from "./services/fallbackQueue.js";
import userRoutes from "./routes/userRoutes.js";
import postRoutes from "./routes/postRoutes.js";
import "./models/index.js";
import { errorHandler, notFoundHandler } from "./middleware/errorHandler.js";
import {
  markServiceReady,
  markServiceNotReady,
  getServiceStatus,
  areCriticalServicesReady,
} from "./config/serviceStatus.js";
import { requireServicesReady } from "./middleware/serviceReady.js";

if (!process.env.NODE_ENV) {
  process.env.NODE_ENV = "development";
}

dotenv.config({ quiet: true });

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.use("/api/posts", requireServicesReady);
app.use("/api/users", requireServicesReady);

app.use("/api/users", userRoutes);
app.use("/api/posts", postRoutes);

app.get("/", (req, res) => {
  res.json({ message: "Instagram Feed API is running!" });
});

// Add readiness endpoint BEFORE other routes
app.get("/api/ready", (req, res) => {
  const status = getServiceStatus();
  const ready = areCriticalServicesReady();

  res.status(ready ? 200 : 503).json({
    ready,
    status: status.services,
    errors: status.errors,
    timestamp: new Date().toISOString(),
  });
});

// Enhanced health check with service status
app.get("/api/health", async (req, res) => {
  try {
    const { getMetrics, getRedisMemoryInfo } = await import(
      "./services/monitoring.js"
    );
    const metrics = getMetrics();
    const redisMemory = await getRedisMemoryInfo();
    const serviceStatus = getServiceStatus();

    res.json({
      status: areCriticalServicesReady() ? "healthy" : "degraded",
      timestamp: new Date().toISOString(),
      services: serviceStatus.services,
      metrics,
      redis: redisMemory,
    });
  } catch (error) {
    res.status(500).json({
      status: "unhealthy",
      error: error.message,
    });
  }
});

// Error handling middleware (must be last)
app.use(notFoundHandler);
app.use(errorHandler);


app.get("/api/redis/test", async (req, res) => {
  try {
    // Test SET and GET operations
    await redisClient.set("test:key", "Hello Redis!");
    const value = await redisClient.get("test:key");

    // Get Redis info
    const info = await redisClient.info("server");

    res.json({
      success: true,
      message: "Redis connection is working!",
      test: {
        key: "test:key",
        value: value,
      },
      info: info.split("\n").slice(0, 5),
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: "Redis connection failed",
      error: error.message,
    });
  }
});

app.get("/api/kafka/test", async (req, res) => {
  try {
    const { isKafkaAvailable } = await import("./config/kafka.js");
    const { sendMessage } = await import("./services/kafkaProducer.js");
    const { TOPICS } = await import("./config/kafka.js");

    if (!isKafkaAvailable()) {
      return res.status(503).json({
        success: false,
        message: "Kafka is not available",
        error: "Kafka broker is not connected",
      });
    }

    // Send a test message
    const testMessage = {
      eventType: "TEST",
      message: "This is a test message from the API",
      timestamp: new Date().toISOString(),
    };

    const result = await sendMessage(
      TOPICS.POST_CREATED,
      testMessage,
      "test-key"
    );

    res.json({
      success: true,
      message: "Kafka connection is working!",
      test: {
        topic: TOPICS.POST_CREATED,
        result: result,
      },
      kafkaAvailable: isKafkaAvailable(),
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: "Kafka connection failed",
      error: error.message,
    });
  }
});

async function startServer() {
  // Start HTTP server IMMEDIATELY
  const server = app.listen(PORT, () => {
    console.log(`🚀 Server running on http://localhost:${PORT}`);
    console.log("⏳ Services initializing in background...");
    console.log("   Check /api/ready endpoint for service status");
  });

  // Initialize services in background with status tracking
  initializeServices().catch((error) => {
    console.error("❌ Error during service initialization:", error);
    // Don't exit - server is already running
  });

  return server;
}

async function initializeServices() {
  try {
    console.log("🔌 Connecting to databases...");

    // PostgreSQL
    try {
      await sequelize.authenticate();
      markServiceReady("postgresql");
      console.log("✅ Connected to PostgreSQL");
      await sequelize.sync({ alter: false });
    } catch (error) {
      markServiceNotReady("postgresql", error);
      console.error("❌ PostgreSQL initialization failed:", error.message);
      throw error; // Critical service - fail fast
    }
    try {
      await initializeRedis();
      markServiceReady("redis");
      console.log("✅ Connected to Redis");
      await loadLuaScripts();
    } catch (error) {
      markServiceNotReady("redis", error);
      console.error("❌ Redis initialization failed:", error.message);
      throw error; // Critical service - fail fast
    }

    // Start fallback queue worker
    startFallbackWorker();

    // Kafka (optional - has fallback) - Start in background
    console.log("🔌 Connecting to Kafka...");

    // Don't await - let Kafka initialize in background
    initializeKafkaAsync().catch((error) => {
      markServiceNotReady("kafka", error);
      console.warn("⚠️ Kafka initialization failed:", error.message);
    });

    // Final status check
    const status = getServiceStatus();
    if (status.criticalReady) {
      console.log("\n✅ All critical services are ready!");
      console.log("   Services status:", status.services);
    } else {
      console.warn("\n⚠️ Some critical services are not ready");
      console.warn("   Services status:", status.services);
    }
  } catch (error) {
    console.error("❌ Error during service initialization:", error);
    throw error;
  }
}

// Separate async function for Kafka initialization
async function initializeKafkaAsync() {
  try {
    await initializeKafka();
    await connectProducer();

    // Start consumer in background (non-blocking)
    startFeedConsumer()
      .then(() => {
        markServiceReady("kafka");
        console.log("✅ Kafka initialized and consumer started");
      })
      .catch((error) => {
        markServiceNotReady("kafka", error);
        throw error;
      });

    // Don't wait for consumer to fully join - it will join in background
  } catch (kafkaError) {
    markServiceNotReady("kafka", kafkaError);
    throw kafkaError;
  }
}

// Graceful shutdown
async function gracefulShutdown(exitCode = 0) {
  console.log("🛑 Shutting down gracefully...");

  try {
    // Stop Kafka consumer
    await stopFeedConsumer();
    // Disconnect Kafka producer
    await disconnectProducer();
    // Disconnect Kafka admin
    await disconnectKafka();
    console.log("✅ Kafka connections closed");
  } catch (error) {
    console.error("❌ Error during Kafka shutdown:", error);
  }

  process.exit(exitCode);
}

// Handle unhandled promise rejections
process.on("unhandledRejection", (reason, promise) => {
  console.error("❌ Unhandled Promise Rejection:", reason);
  console.error("Promise:", promise);
  // Log the error but don't crash
  // The error should be handled by the error middleware
  // In production, you might want to send this to a logging service
  // DO NOT exit or throw - just log

  // Additional safety: log stack trace if available
  if (reason && reason.stack) {
    console.error("Stack:", reason.stack);
  }
});

// Handle uncaught exceptions
process.on("uncaughtException", async (error) => {
  console.error("❌ Uncaught Exception:", error);
  console.error("Stack:", error.stack);
  console.error("Error name:", error.name);
  console.error("Error message:", error.message);

  // ALWAYS log but NEVER exit in development
  // This prevents nodemon from restarting on every error
  if (process.env.NODE_ENV !== "production") {
    console.error(
      "⚠️ Uncaught exception in development mode - server will continue"
    );
    console.error("   Fix the error to prevent potential issues");
    // Log to monitoring service if available
    return;
  }

  // In production, only exit for truly fatal errors
  // Most errors should be handled gracefully
  if (error.name === "FatalError" || error.code === "EADDRINUSE") {
    console.error("❌ Fatal error detected, shutting down...");
    await gracefulShutdown(1);
  } else {
    console.error("⚠️ Non-fatal uncaught exception, continuing...");
  }
});

process.on("SIGTERM", () => gracefulShutdown(0));
process.on("SIGINT", () => gracefulShutdown(0));

startServer();
