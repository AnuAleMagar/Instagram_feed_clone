/**
 * Centralized Error Handling Middleware
 * Provides consistent error responses across the application
 */

import { ERROR_CONFIG } from "../config/constants.js";

/**
 * Custom error class for application errors
 */
export class AppError extends Error {
  constructor(message, statusCode = 500, isOperational = true) {
    super(message);
    this.statusCode = statusCode;
    this.isOperational = isOperational;
    Error.captureStackTrace(this, this.constructor);
  }
}

/**
 * Check if error is retryable
 * @param {Error} error - Error object
 * @returns {boolean} True if error is retryable
 */
export function isRetryableError(error) {
  if (!error) return false;

  const errorCode = error.code || error.cause?.code || "";
  return ERROR_CONFIG.RETRYABLE_ERRORS.some(
    (retryableCode) =>
      errorCode === retryableCode || error.message?.includes(retryableCode)
  );
}

/**
 * Format error message for logging
 * @param {Error} error - Error object
 * @returns {string} Formatted error message
 */
function formatErrorMessage(error) {
  let message = error.message || "Unknown error";

  // Truncate if too long
  if (message.length > ERROR_CONFIG.MAX_ERROR_LOG_LENGTH) {
    message = message.substring(0, ERROR_CONFIG.MAX_ERROR_LOG_LENGTH) + "...";
  }

  return message;
}

/**
 * Global error handler middleware
 * Should be used as the last middleware in Express app
 */
export function errorHandler(err, req, res, next) {
  // Default error
  let statusCode = 500;
  let message = "Internal server error";
  let error = err;

  // Handle known error types
  if (err instanceof AppError) {
    statusCode = err.statusCode;
    message = err.message;
  } else if (err.name === "ValidationError") {
    statusCode = 400;
    message = err.message || "Validation error";
  } else if (err.name === "UnauthorizedError") {
    statusCode = 401;
    message = "Unauthorized";
  } else if (err.code === "ECONNREFUSED") {
    statusCode = 503;
    message = "Service temporarily unavailable";
  } else if (err.message) {
    message = formatErrorMessage(err);
  }

  // Log error
  const logLevel = statusCode >= 500 ? "error" : "warn";
  console[logLevel](
    `[${statusCode}] ${req.method} ${req.path}:`,
    message,
    err.stack && process.env.NODE_ENV === "development" ? err.stack : ""
  );

  // Send error response
  res.status(statusCode).json({
    success: false,
    message,
    ...(process.env.NODE_ENV === "development" && {
      error: {
        name: err.name,
        stack: err.stack,
      },
    }),
  });
}

/**
 * Async handler wrapper to catch errors in async route handlers
 * @param {Function} fn - Async route handler function
 * @returns {Function} Wrapped function
 */
export function asyncHandler(fn) {
  return (req, res, next) => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
}

/**
 * 404 Not Found handler
 */
export function notFoundHandler(req, res, next) {
  res.status(404).json({
    success: false,
    message: `Route ${req.method} ${req.path} not found`,
  });
}
