import {
  Collection,
  WatermarkConfig,
  WatermarkProgress,
} from "../../models/collection.model";
import {
  GetObjectCommand,
  PutObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
} from "@aws-sdk/client-s3";
import { processImageWithWatermark } from "./watermark.proccessor";
import { WatermarkedFile } from "../../models/watermarkedfile.model";
import { redis } from "./queue";
import { s3Client } from "./s3";
import { File } from "../../models/file.model";
import logger from "../../utils/logger";
import { cpus } from "os";

logger.info("🚀 Watermark worker started with batch processing!");

// ===========================
// Type Definitions
// ===========================

interface WatermarkJobData {
  collectionId: string;
  slug: string;
  watermarkConfig: WatermarkConfig;
}

interface BatchProcessingOptions {
  batchSize: number;
  maxConcurrency: number;
  retryAttempts: number;
  retryDelay: number;
}

interface ImageProcessingResult {
  success: boolean;
  fileId: string;
  originalKey: string;
  processedKey?: string;
  size?: number;
  error?: Error;
}

interface ProcessingMetrics {
  startTime: number;
  processedCount: number;
  failedCount: number;
  totalSize: number;
}

// ===========================
// Configuration
// ===========================

// Load batch processing configuration from environment variables
const getBatchOptions = (): BatchProcessingOptions => ({
  batchSize: parseInt(process.env.BATCH_SIZE || "10"),
  maxConcurrency: parseInt(
    process.env.MAX_CONCURRENCY || String(Math.max(1, cpus().length - 1))
  ),
  retryAttempts: parseInt(process.env.RETRY_ATTEMPTS || "3"),
  retryDelay: parseInt(process.env.RETRY_DELAY || "1000"),
});

// ===========================
// Utility Functions
// ===========================

/**
 * Creates chunks of items for batch processing
 * @param items - Array of items to chunk
 * @param chunkSize - Size of each chunk
 * @returns Array of chunks
 */
const createChunks = <T>(items: T[], chunkSize: number): T[][] => {
  const chunks: T[][] = [];
  for (let i = 0; i < items.length; i += chunkSize) {
    chunks.push(items.slice(i, i + chunkSize));
  }
  return chunks;
};

/**
 * Delays execution for specified milliseconds
 * @param ms - Milliseconds to delay
 */
const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

// ===========================
// S3 Operations
// ===========================

/**
 * Downloads an image from S3
 * @param key - S3 object key
 * @returns Image buffer
 */
const getImageFromS3 = async (key: string): Promise<Buffer> => {
  try {
    const command = new GetObjectCommand({
      Bucket: process.env.AWS_BUCKET_NAME!,
      Key: key,
    });

    const response = await s3Client.send(command);

    if (!response.Body) {
      throw new Error("No file content received from S3");
    }

    // Convert stream to buffer efficiently
    const chunks: Uint8Array[] = [];
    const reader = response.Body.transformToWebStream().getReader();

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
    }

    return Buffer.concat(chunks);
  } catch (error) {
    logger.error(`Error getting image from S3 with key ${key}:`, error);
    throw error;
  }
};

/**
 * Uploads processed image to S3 with optimizations
 * @param buffer - Image buffer to upload
 * @param key - S3 object key
 * @param contentType - MIME type of the image
 */
const uploadProcessedImage = async (
  buffer: Buffer,
  key: string,
  contentType: string = "image/jpeg"
): Promise<void> => {
  try {
    const command = new PutObjectCommand({
      Bucket: process.env.AWS_BUCKET_NAME!,
      Key: key,
      Body: buffer,
      ContentType: contentType,
      CacheControl: "max-age=31536000", // Cache for 1 year
    });

    await s3Client.send(command);
    logger.info(`Uploaded processed image to: ${key}`);
  } catch (error) {
    logger.error(`Error uploading processed image with key ${key}:`, error);
    throw error;
  }
};

/**
 * Deletes all watermarked files for a collection from S3 and database
 * @param collectionId - Collection ID to clean up
 */
const deleteWatermarkedDirectory = async (
  collectionId: string
): Promise<void> => {
  try {
    const prefix = `watermarked-${collectionId}/`;

    // Delete from database first
    await WatermarkedFile.deleteMany({ collectionId });

    // List and delete S3 objects in batches to avoid overwhelming the API
    let continuationToken: string | undefined;
    const deletePromises: Promise<any>[] = [];

    do {
      const listCommand = new ListObjectsV2Command({
        Bucket: process.env.AWS_BUCKET_NAME!,
        Prefix: prefix,
        MaxKeys: 1000, // S3 max keys per request
        ContinuationToken: continuationToken,
      });

      const listResponse = await s3Client.send(listCommand);
      continuationToken = listResponse.NextContinuationToken;

      if (listResponse.Contents && listResponse.Contents.length > 0) {
        // Create delete operations for each object
        const deleteOperations = listResponse.Contents.filter(
          (obj) => obj.Key
        ).map((obj) => {
          const deleteCommand = new DeleteObjectCommand({
            Bucket: process.env.AWS_BUCKET_NAME!,
            Key: obj.Key!,
          });
          return s3Client.send(deleteCommand);
        });

        deletePromises.push(...deleteOperations);

        // Process deletions in batches of 100 to avoid rate limits
        if (deletePromises.length >= 100) {
          await Promise.all(deletePromises);
          deletePromises.length = 0; // Clear array
        }
      }
    } while (continuationToken);

    // Process any remaining deletions
    if (deletePromises.length > 0) {
      await Promise.all(deletePromises);
    }

    logger.info(`Deleted watermarked directory: ${prefix}`);
  } catch (error) {
    logger.error(
      `Error deleting watermarked directory for collection ${collectionId}:`,
      error
    );
    throw error;
  }
};

// ===========================
// Database Operations
// ===========================

/**
 * Updates the watermark progress in the database
 * @param collectionId - Collection ID
 * @param progress - Progress updates to apply
 * @param options - Additional options
 */
const updateWatermarkProgress = async (
  collectionId: string,
  progress: Partial<WatermarkProgress>,
  options: { skipEstimation?: boolean } = {}
): Promise<void> => {
  try {
    const collection = await Collection.findById(collectionId);
    if (!collection) {
      throw new Error(`Collection not found: ${collectionId}`);
    }

    const currentProgress = collection.watermarkProgress || {
      total: 0,
      watermarked: 0,
      locked: false,
      status: "idle" as const,
    };

    // Calculate estimated time remaining based on average processing speed
    let estimatedTimeRemaining: number | undefined;
    if (
      !options.skipEstimation &&
      progress.watermarked !== undefined &&
      currentProgress.startedAt &&
      progress.watermarked > 0 &&
      currentProgress.total > progress.watermarked
    ) {
      const elapsedTime =
        (Date.now() - currentProgress.startedAt.getTime()) / 1000;
      const averageTimePerImage = elapsedTime / progress.watermarked;
      const remainingImages = currentProgress.total - progress.watermarked;
      estimatedTimeRemaining = Math.round(
        averageTimePerImage * remainingImages
      );
    }

    // Merge current progress with updates
    const updatedProgress = {
      ...currentProgress,
      ...progress,
      estimatedTimeRemaining:
        estimatedTimeRemaining ?? progress.estimatedTimeRemaining,
    };

    collection.setWatermarkProgress(updatedProgress);
    await collection.save();

    logger.debug(
      `Updated watermark progress for collection ${collectionId}:`,
      updatedProgress
    );
  } catch (error) {
    logger.error(
      `Error updating watermark progress for collection ${collectionId}:`,
      error
    );
    throw error;
  }
};

/**
 * Validates if a collection is still valid for processing
 * @param collectionId - Collection ID to validate
 * @returns True if collection can be processed
 */
const validateCollectionForProcessing = async (
  collectionId: string
): Promise<boolean> => {
  try {
    const collection = await Collection.findById(collectionId);
    if (!collection) {
      logger.warn(
        `Collection ${collectionId} no longer exists, skipping processing`
      );
      return false;
    }

    // Check if processing was cancelled or already completed
    const progress = collection.watermarkProgress;
    if (
      !progress ||
      progress.status === "idle" ||
      progress.status === "completed"
    ) {
      logger.warn(
        `Collection ${collectionId} status changed to ${
          progress?.status || "idle"
        }, skipping processing`
      );
      return false;
    }

    return true;
  } catch (error) {
    logger.error(`Error validating collection ${collectionId}:`, error);
    return false;
  }
};

// ===========================
// Image Processing Functions
// ===========================

/**
 * Processes a single image with watermark
 * @param image - Image document from database
 * @param collectionId - Collection ID
 * @param watermarkConfig - Watermark configuration
 * @param watermarkImageBuffer - Optional watermark image buffer
 * @returns Processing result
 */
const processSingleImage = async (
  image: any,
  collectionId: string,
  watermarkConfig: WatermarkConfig,
  watermarkImageBuffer?: Buffer
): Promise<ImageProcessingResult> => {
  const startTime = Date.now();

  try {
    // Download original image from S3
    const imageBuffer = await getImageFromS3(image.key);

    // Apply watermark to image
    const processedBuffer = await processImageWithWatermark({
      inputBuffer: imageBuffer,
      watermarkConfig,
      watermarkImageBuffer,
    });

    // Generate filename and S3 key for processed image
    const originalFileName = image.key.split("/").pop() || `image-${image._id}`;
    const processedImageKey = `watermarked-${collectionId}/${originalFileName}`;

    // Upload processed image to S3
    await uploadProcessedImage(
      processedBuffer,
      processedImageKey,
      image.type || "image/jpeg"
    );

    // Save watermarked file record to database
    await WatermarkedFile.create({
      name: originalFileName,
      key: processedImageKey,
      type: image.type,
      size: processedBuffer.length,
      collectionId,
      originalFileId: image._id,
      workspaceId: image.workspaceId,
    });

    const processingTime = Date.now() - startTime;
    logger.debug(`Processed image ${image.key} in ${processingTime}ms`);

    return {
      success: true,
      fileId: image._id,
      originalKey: image.key,
      processedKey: processedImageKey,
      size: processedBuffer.length,
    };
  } catch (error) {
    logger.error(`Failed to process image ${image.key}:`, error);
    return {
      success: false,
      fileId: image._id,
      originalKey: image.key,
      error: error as Error,
    };
  }
};

/**
 * Processes a single image with retry logic
 * @param image - Image to process
 * @param collectionId - Collection ID
 * @param watermarkConfig - Watermark configuration
 * @param watermarkImageBuffer - Optional watermark image buffer
 * @param options - Retry options
 * @returns Processing result
 */
const processImageWithRetry = async (
  image: any,
  collectionId: string,
  watermarkConfig: WatermarkConfig,
  watermarkImageBuffer: Buffer | undefined,
  options: BatchProcessingOptions
): Promise<ImageProcessingResult> => {
  let lastError: Error | undefined;

  // Attempt processing with exponential backoff on failure
  for (let attempt = 0; attempt < options.retryAttempts; attempt++) {
    try {
      return await processSingleImage(
        image,
        collectionId,
        watermarkConfig,
        watermarkImageBuffer
      );
    } catch (error) {
      lastError = error as Error;
      logger.warn(
        `Attempt ${attempt + 1}/${options.retryAttempts} failed for image ${
          image.key
        }:`,
        error
      );

      // Don't delay after the last attempt
      if (attempt < options.retryAttempts - 1) {
        // Exponential backoff: delay * 2^attempt
        const backoffDelay = options.retryDelay * Math.pow(2, attempt);
        await delay(backoffDelay);
      }
    }
  }

  // All attempts failed
  return {
    success: false,
    fileId: image._id,
    originalKey: image.key,
    error: lastError,
  };
};

/**
 * Processes a batch of images in parallel
 * @param images - Array of images to process
 * @param collectionId - Collection ID
 * @param watermarkConfig - Watermark configuration
 * @param watermarkImageBuffer - Optional watermark image buffer
 * @param options - Batch processing options
 * @returns Array of processing results
 */
const processBatch = async (
  images: any[],
  collectionId: string,
  watermarkConfig: WatermarkConfig,
  watermarkImageBuffer: Buffer | undefined,
  options: BatchProcessingOptions
): Promise<ImageProcessingResult[]> => {
  // Split images into chunks for controlled concurrency
  const chunks = createChunks(images, options.maxConcurrency);
  const results: ImageProcessingResult[] = [];

  // Process each chunk sequentially, but images within chunk in parallel
  for (const chunk of chunks) {
    const chunkResults = await Promise.all(
      chunk.map((image) =>
        processImageWithRetry(
          image,
          collectionId,
          watermarkConfig,
          watermarkImageBuffer,
          options
        )
      )
    );
    results.push(...chunkResults);
  }

  return results;
};

// ===========================
// Main Job Processing
// ===========================

/**
 * Processes a complete watermark job for a collection
 * @param jobData - Job data from the queue
 * @returns Processing result summary
 */
const processWatermarkJob = async (jobData: WatermarkJobData) => {
  let hasStartedProcessing = false;
  const options = getBatchOptions();

  // Initialize metrics for tracking performance
  const metrics: ProcessingMetrics = {
    startTime: Date.now(),
    processedCount: 0,
    failedCount: 0,
    totalSize: 0,
  };

  try {
    logger.info(
      "Processing watermark job for collection:",
      jobData.collectionId
    );

    const { collectionId, watermarkConfig, slug } = jobData;

    // Validate collection is still valid for processing
    if (!(await validateCollectionForProcessing(collectionId))) {
      return {
        success: false,
        collectionId,
        reason: "Collection no longer valid for processing",
        skipped: true,
      };
    }

    // Fetch all images for this collection
    const images = await File.find({ collectionSlug: slug }).lean();
    logger.info(`Found ${images.length} images to process`);

    // Handle empty collection
    if (images.length === 0) {
      await updateWatermarkProgress(collectionId, {
        status: "completed",
        locked: false,
        completedAt: new Date(),
      });

      return {
        success: true,
        collectionId,
        processedImages: 0,
        processedAt: new Date().toISOString(),
        message: "No images found to process",
      };
    }

    // Clean up existing watermarked content if re-processing
    const collection = await Collection.findById(collectionId);
    if (collection?.watermarkProgress?.status === "completed") {
      logger.info("Deleting existing watermarked directory...");
      await deleteWatermarkedDirectory(collectionId);
    }

    // Initialize processing state
    hasStartedProcessing = true;
    await updateWatermarkProgress(collectionId, {
      total: images.length,
      watermarked: 0,
      locked: true,
      status: "processing",
      startedAt: new Date(),
      completedAt: undefined,
      estimatedTimeRemaining: undefined,
      currentImageName: `Processing ${images.length} images in batches...`,
    });

    // Download watermark image once if needed
    let watermarkImageBuffer: Buffer | undefined;
    if (watermarkConfig.type === "image" && watermarkConfig.imageKey) {
      logger.info("Downloading watermark image from S3...");
      watermarkImageBuffer = await getImageFromS3(watermarkConfig.imageKey);
    }

    // Calculate batch information
    const totalBatches = Math.ceil(images.length / options.batchSize);
    logger.info(
      `Processing ${images.length} images in ${totalBatches} batches`
    );

    // Process images in batches
    for (let i = 0; i < images.length; i += options.batchSize) {
      const batchNumber = Math.floor(i / options.batchSize) + 1;
      const batch = images.slice(i, i + options.batchSize);

      logger.info(
        `Processing batch ${batchNumber}/${totalBatches} (${batch.length} images)`
      );

      // Check if processing should continue
      if (!(await validateCollectionForProcessing(collectionId))) {
        logger.warn(
          `Collection ${collectionId} no longer valid, stopping at batch ${batchNumber}`
        );
        break;
      }

      // Update progress for current batch
      await updateWatermarkProgress(collectionId, {
        currentImageName: `Processing batch ${batchNumber}/${totalBatches}`,
        watermarked: metrics.processedCount,
      });

      // Process current batch
      const batchResults = await processBatch(
        batch,
        collectionId,
        watermarkConfig,
        watermarkImageBuffer,
        options
      );

      // Update metrics based on batch results
      batchResults.forEach((result) => {
        if (result.success) {
          metrics.processedCount++;
          metrics.totalSize += result.size || 0;
        } else {
          metrics.failedCount++;
        }
      });

      // Update progress after batch completion
      const successfulInBatch = batchResults.filter((r) => r.success).length;
      await updateWatermarkProgress(collectionId, {
        watermarked: metrics.processedCount,
        currentImageName: `Completed batch ${batchNumber}/${totalBatches} (${successfulInBatch}/${batch.length} successful)`,
      });

      // Log any failures in the batch
      const failedInBatch = batchResults.filter((r) => !r.success);
      if (failedInBatch.length > 0) {
        logger.warn(
          `Batch ${batchNumber} had ${failedInBatch.length} failures:`,
          failedInBatch.map((f) => ({
            key: f.originalKey,
            error: f.error?.message,
          }))
        );
      }
    }

    // Calculate final performance metrics
    const totalTime = Date.now() - metrics.startTime;
    const averageTimePerImage = totalTime / images.length;
    const averageSizePerImage = metrics.totalSize / metrics.processedCount || 0;

    // Determine final status based on results
    const finalStatus =
      metrics.failedCount === 0
        ? "completed"
        : metrics.processedCount === 0
        ? "failed"
        : "completed_with_errors";

    // Update final progress
    await updateWatermarkProgress(collectionId, {
      status: finalStatus as any,
      locked: false,
      completedAt: new Date(),
      currentImageName: undefined,
      estimatedTimeRemaining: 0,
      watermarked: metrics.processedCount,
    });

    // Log completion summary
    logger.info(
      `✅ Watermark processing completed for collection: ${collectionId}`,
      {
        totalImages: images.length,
        processedImages: metrics.processedCount,
        failedImages: metrics.failedCount,
        totalTime: `${(totalTime / 1000).toFixed(2)}s`,
        averageTimePerImage: `${(averageTimePerImage / 1000).toFixed(2)}s`,
        averageSizePerImage: `${(averageSizePerImage / 1024 / 1024).toFixed(
          2
        )}MB`,
        throughput: `${(images.length / (totalTime / 1000)).toFixed(
          2
        )} images/second`,
      }
    );

    return {
      success: true,
      collectionId,
      processedImages: metrics.processedCount,
      failedImages: metrics.failedCount,
      totalSize: metrics.totalSize,
      processingTime: totalTime,
      processedAt: new Date().toISOString(),
    };
  } catch (error) {
    logger.error("Error processing watermark job:", error);

    // Update progress to failed status only if processing started
    if (hasStartedProcessing) {
      try {
        await updateWatermarkProgress(jobData.collectionId, {
          status: "failed",
          locked: false,
          completedAt: new Date(),
          currentImageName: `Failed: ${(error as Error).message}`,
        });
      } catch (progressError) {
        logger.error("Error updating progress to failed:", progressError);
      }
    }

    throw error;
  }
};

// ===========================
// Worker Management
// ===========================

/**
 * Main worker function that polls Redis queue for jobs
 */
const startWorker = async () => {
  const queueKey = "watermark-processing";
  const options = getBatchOptions();

  logger.info("Starting Redis worker with batch processing support...");
  logger.info(`Configuration:`, {
    batchSize: options.batchSize,
    maxConcurrency: options.maxConcurrency,
    retryAttempts: options.retryAttempts,
  });

  let consecutiveErrors = 0;
  const maxConsecutiveErrors = 5;
  const baseRetryDelay = 1000; // 1 second

  // Main worker loop
  while (true) {
    try {
      // Block and wait for job from Redis queue (with 30s timeout)
      const result = await redis.brpop(queueKey, 30);

      if (result && result[1]) {
        // Parse and process job
        const jobData: WatermarkJobData = JSON.parse(result[1]);
        logger.info(
          `🔄 Processing job for collection: ${jobData.collectionId}`
        );

        const processingResult = await processWatermarkJob(jobData);

        if (processingResult.success) {
          logger.info(
            `✅ Job completed successfully for collection: ${jobData.collectionId}`,
            {
              processed: processingResult.processedImages,
              failed: processingResult.failedImages,
              time: `${(processingResult.processingTime! / 1000).toFixed(2)}s`,
            }
          );
          consecutiveErrors = 0; // Reset error counter on success
        } else {
          logger.warn(
            `⚠️ Job skipped for collection: ${jobData.collectionId} - ${processingResult.reason}`
          );
        }
      } else {
        // No job received within timeout - normal behavior
        logger.debug("No jobs in queue, continuing to poll...");
        consecutiveErrors = 0;
      }
    } catch (error) {
      consecutiveErrors++;
      logger.error(
        `❌ Error processing task from Redis (${consecutiveErrors}/${maxConsecutiveErrors}):`,
        error
      );

      // Implement exponential backoff for consecutive errors
      if (consecutiveErrors >= maxConsecutiveErrors) {
        const retryDelay =
          baseRetryDelay *
          Math.pow(2, consecutiveErrors - maxConsecutiveErrors);
        logger.error(
          `Too many consecutive errors, waiting ${retryDelay}ms before retry...`
        );
        await delay(retryDelay);
      } else {
        await delay(baseRetryDelay);
      }
    }
  }
};

/**
 * Handles graceful shutdown of the worker
 * @param signal - Signal that triggered shutdown
 */
const gracefulShutdown = async (signal: string) => {
  logger.info(`Received ${signal}, shutting down worker gracefully...`);

  try {
    // Log queue status before shutdown
    const queueLength = await redis.llen("watermark-processing");
    logger.info(`Queue length at shutdown: ${queueLength}`);

    // Close Redis connection
    await redis.quit();
    logger.info("Redis connection closed");
  } catch (error) {
    logger.error("Error during graceful shutdown:", error);
  }

  process.exit(0);
};

// ===========================
// Worker Initialization
// ===========================

// Start the worker
startWorker().catch((err) => {
  logger.error("Worker failed to start:", err);
  process.exit(1);
});

// Register shutdown handlers
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

// Handle unexpected errors
process.on("uncaughtException", (error) => {
  logger.error("Uncaught Exception:", error);
  process.exit(1);
});

process.on("unhandledRejection", (reason, promise) => {
  logger.error("Unhandled Rejection at:", promise, "reason:", reason);
  process.exit(1);
});

export default startWorker;
