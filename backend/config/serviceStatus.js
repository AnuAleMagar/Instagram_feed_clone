/**
 * Service Status Tracker
 * Tracks the connection status of all required services
 */

let serviceStatus = {
  postgresql: false,
  redis: false,
  kafka: false,
};

let serviceErrors = {
  postgresql: null,
  redis: null,
  kafka: null,
};

/**
 * Check if all critical services are ready
 * Critical services: PostgreSQL, Redis
 * Kafka is optional (has fallback queue)
 */
export function areCriticalServicesReady() {
  return (
    serviceStatus.postgresql && serviceStatus.redis
  );
}

/**
 * Check if all services (including optional) are ready
 */
export function areAllServicesReady() {
  return (
    serviceStatus.postgresql &&
    serviceStatus.redis &&
    serviceStatus.kafka
  );
}

/**
 * Get service status
 */
export function getServiceStatus() {
  return {
    services: { ...serviceStatus },
    errors: { ...serviceErrors },
    criticalReady: areCriticalServicesReady(),
    allReady: areAllServicesReady(),
  };
}

/**
 * Mark a service as ready
 */
export function markServiceReady(serviceName) {
  serviceStatus[serviceName] = true;
  serviceErrors[serviceName] = null;
  console.log(`✅ Service status: ${serviceName} is ready`);
}

/**
 * Mark a service as not ready
 */
export function markServiceNotReady(serviceName, error = null) {
  serviceStatus[serviceName] = false;
  serviceErrors[serviceName] = error;
  console.warn(
    `⚠️ Service status: ${serviceName} is not ready`,
    error?.message
  );
}

/**
 * Check if a specific service is ready
 */
export function isServiceReady(serviceName) {
  return serviceStatus[serviceName] === true;
}
