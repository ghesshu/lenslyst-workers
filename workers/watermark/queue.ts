import Redis from "ioredis";
import logger from "../../utils/logger";

// Validate Redis connection string
const redisUrl =
  process.env.REDIS_URL ||
  "redis://default:CkaBvOBGUlZwznOek5btVkaCz36lc0pk9Fzw6jrOqs2wcQGqlXqph8oih2LVJk3o@147.79.100.180:5422/0";
if (!redisUrl) {
  throw new Error("REDIS_URL environment variable is not set");
}

// Initialize Redis client with connection string
export const redis = new Redis(redisUrl, {
  maxRetriesPerRequest: 3,
  retryStrategy: (times: number) => {
    const delay = Math.min(times * 100, 5000);
    logger.info(
      `Retrying Redis connection (attempt ${times}) after ${delay}ms`
    );
    return delay;
  },
  connectTimeout: 10000, // 10 seconds timeout for initial connection
});

// Log connection errors
redis.on("error", (error: Error) => {
  logger.error("Redis connection error:", error.message);
});

// Log successful connection
redis.on("connect", () => {
  logger.info("Successfully connected to Redis");
});

// Ensure Redis is ready before proceeding
redis.on("ready", () => {
  logger.info("Redis client is ready");
});
