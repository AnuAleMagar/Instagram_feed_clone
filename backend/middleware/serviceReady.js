/**
 * Middleware to check if services are ready before processing requests
 */

import {
  areCriticalServicesReady,
  getServiceStatus,
} from "../config/serviceStatus.js";

/**
 * Middleware to ensure critical services are ready
 * Returns 503 if services are not ready
 */
export function requireServicesReady(req, res, next) {
  if (!areCriticalServicesReady()) {
    const status = getServiceStatus();
    return res.status(503).json({
      success: false,
      message:
        "Service temporarily unavailable - services are still initializing",
      status: status.services,
      errors: status.errors,
      timestamp: new Date().toISOString(),
    });
  }
  next();
}

/**
 * Optional: Only apply to critical endpoints
 * Use this for endpoints that require all services
 */
export async function requireAllServicesReady(req, res, next) {
  const { areAllServicesReady, getServiceStatus } = await import(
    "../config/serviceStatus.js"
  );

  if (!areAllServicesReady()) {
    const status = getServiceStatus();
    return res.status(503).json({
      success: false,
      message:
        "Service temporarily unavailable - some services are still initializing",
      status: status.services,
      errors: status.errors,
      timestamp: new Date().toISOString(),
    });
  }
  next();
}
