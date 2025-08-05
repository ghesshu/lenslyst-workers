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

logger.info("🚀 Watermark worker started!");

// Define the job data type
interface WatermarkJobData {
  collectionId: string;
  slug: string;
  watermarkConfig: WatermarkConfig;
}

// Function to get image from S3
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

    // Convert stream to buffer
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

// Function to upload processed image to S3
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
    });

    await s3Client.send(command);
    logger.info(`Uploaded processed image to: ${key}`);
  } catch (error) {
    logger.error(`Error uploading processed image with key ${key}:`, error);
    throw error;
  }
};

// Function to delete existing watermarked directory and database entries
const deleteWatermarkedDirectory = async (
  collectionId: string
): Promise<void> => {
  try {
    const prefix = `watermarked-${collectionId}/`;

    // Delete watermarked files from database
    await WatermarkedFile.deleteMany({ collectionId });
    logger.info(
      `Deleted watermarked files from database for collection: ${collectionId}`
    );

    // List all objects in the directory
    const listCommand = new ListObjectsV2Command({
      Bucket: process.env.AWS_BUCKET_NAME!,
      Prefix: prefix,
    });

    const listResponse = await s3Client.send(listCommand);

    if (listResponse.Contents && listResponse.Contents.length > 0) {
      // Delete all objects in the directory
      for (const object of listResponse.Contents) {
        if (object.Key) {
          const deleteCommand = new DeleteObjectCommand({
            Bucket: process.env.AWS_BUCKET_NAME!,
            Key: object.Key,
          });
          await s3Client.send(deleteCommand);
          logger.info(`Deleted: ${object.Key}`);
        }
      }
      logger.info(
        `Deleted ${listResponse.Contents.length} files from S3 for collection: ${collectionId}`
      );
    } else {
      logger.info(
        `No existing watermarked files found in S3 for collection: ${collectionId}`
      );
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

// Function to update watermark progress with enhanced tracking
const updateWatermarkProgress = async (
  collectionId: string,
  progress: Partial<WatermarkProgress>
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

    // Calculate estimated time remaining
    let estimatedTimeRemaining: number | undefined;
    if (
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

    const updatedProgress = {
      ...currentProgress,
      ...progress,
      estimatedTimeRemaining:
        estimatedTimeRemaining || progress.estimatedTimeRemaining,
    };

    collection.setWatermarkProgress(updatedProgress);
    await collection.save();

    logger.info(
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

// Function to check if collection still exists and is valid for processing
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

    // Check if collection was manually cancelled or reset
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

// Function to process a watermark job with enhanced queue management
const processWatermarkJob = async (jobData: WatermarkJobData) => {
  let hasStartedProcessing = false;

  try {
    logger.info(
      "Processing watermark job for collection:",
      jobData.collectionId
    );
    logger.info("Job data:", JSON.stringify(jobData, null, 2));

    const { collectionId, watermarkConfig, slug } = jobData;

    // Validate collection before processing
    if (!(await validateCollectionForProcessing(collectionId))) {
      return {
        success: false,
        collectionId,
        reason: "Collection no longer valid for processing",
        skipped: true,
      };
    }

    logger.info(
      `Starting watermark processing for collection: ${collectionId}`
    );
    logger.info(`Watermark type: ${watermarkConfig.type}`);

    if (watermarkConfig.imageKey) {
      logger.info(`Image key: ${watermarkConfig.imageKey}`);
    }

    // Get all images for this collection
    const images = await File.find({ collectionSlug: slug });
    logger.info(`Found ${images.length} images to process`);

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

    // Check if there's existing watermarked content and delete it
    const collection = await Collection.findById(collectionId);
    if (
      collection?.watermarkProgress?.status === "completed" ||
      collection?.watermarkProgress?.status === "failed" ||
      collection?.watermarkProgress?.status === "idle"
    ) {
      logger.info("Deleting existing watermarked directory...");
      await deleteWatermarkedDirectory(collectionId);
    }

    // Always delete existing watermarked content before starting new processing
    logger.info(
      "Deleting existing watermarked directory and database entries..."
    );
    await deleteWatermarkedDirectory(collectionId);

    // Mark as processing and set initial progress
    hasStartedProcessing = true;
    await updateWatermarkProgress(collectionId, {
      total: images.length,
      watermarked: 0,
      locked: true,
      status: "processing",
      startedAt: new Date(),
      completedAt: undefined,
      estimatedTimeRemaining: undefined,
      currentImageName: undefined,
    });

    // Get watermark image if type is image
    let watermarkImageBuffer: Buffer | undefined;

    if (watermarkConfig.type === "image" && watermarkConfig.imageKey) {
      watermarkImageBuffer = await getImageFromS3(watermarkConfig.imageKey);
    }

    // Process each image with progress tracking
    for (let i = 0; i < images.length; i++) {
      const image = images[i];
      const currentImageName =
        image.name || image.key.split("/").pop() || `image-${i + 1}`;

      logger.info(
        `Processing image ${i + 1}/${images.length}: ${currentImageName}`
      );

      // Validate collection is still valid before processing each image
      if (!(await validateCollectionForProcessing(collectionId))) {
        logger.warn(
          `Collection ${collectionId} no longer valid, stopping processing at image ${
            i + 1
          }`
        );
        break;
      }

      // Update current image being processed
      await updateWatermarkProgress(collectionId, {
        currentImageName,
      });

      try {
        // Get image from S3
        const imageBuffer = await getImageFromS3(image.key);

        // Process image with watermark
        const processedBuffer = await processImageWithWatermark({
          inputBuffer: imageBuffer,
          watermarkConfig,
          watermarkImageBuffer,
        });

        // Generate processed image key with unique identifier
        const originalFileName = image.key.split("/").pop() || `image-${i}`;
        const uniqueId = `${Date.now()}-${Math.random()
          .toString(36)
          .substring(2, 15)}`;
        const processedImageKey = `watermarked-${collectionId}/${uniqueId}-${originalFileName}`;

        // Upload processed image to S3
        await uploadProcessedImage(processedBuffer, processedImageKey);

        // Create or update watermarked file entry (FIXED: Using findOneAndUpdate to prevent duplicates)
        const watermarkedFile = await WatermarkedFile.findOneAndUpdate(
          {
            collectionId,
            originalFileId: image._id,
          },
          {
            name: originalFileName,
            key: processedImageKey,
            type: image.type,
            size: processedBuffer.length,
            workspaceId: image.workspaceId,
          },
          {
            upsert: true, // Create if doesn't exist, update if exists
            new: true, // Return the updated document
          }
        );

        logger.info(
          `✅ Processed and saved watermarked file: ${originalFileName} (${
            watermarkedFile.isNew ? "created" : "updated"
          })`
        );

        // Update progress with estimated time
        await updateWatermarkProgress(collectionId, {
          watermarked: i + 1,
        });

        logger.info(
          `✅ Processed image ${i + 1}/${images.length}: ${currentImageName}`
        );
      } catch (error) {
        logger.error(`❌ Error processing image ${image.key}:`, error);

        // Update progress to failed status
        await updateWatermarkProgress(collectionId, {
          status: "failed",
          locked: false,
          completedAt: new Date(),
          currentImageName: `Failed at: ${currentImageName}`,
        });

        throw error;
      }
    }

    // Mark as completed
    await updateWatermarkProgress(collectionId, {
      status: "completed",
      locked: false,
      completedAt: new Date(),
      currentImageName: undefined,
      estimatedTimeRemaining: 0,
    });

    logger.info(
      `✅ Watermark processing completed for collection: ${collectionId}`
    );

    return {
      success: true,
      collectionId,
      processedImages: images.length,
      processedAt: new Date().toISOString(),
    };
  } catch (error) {
    logger.error("Error processing watermark job:", error);

    // Update progress to failed status only if we actually started processing
    if (hasStartedProcessing) {
      try {
        await updateWatermarkProgress(jobData.collectionId, {
          status: "failed",
          locked: false,
          completedAt: new Date(),
        });
      } catch (progressError) {
        logger.error("Error updating progress to failed:", progressError);
      }
    }

    throw error;
  }
};

// Enhanced worker function with better error handling and logging
const startWorker = async () => {
  const queueKey = "watermark-processing";
  logger.info("Starting Redis worker to poll tasks...");

  let consecutiveErrors = 0;
  const maxConsecutiveErrors = 5;
  const baseRetryDelay = 1000; // 1 second

  while (true) {
    try {
      // Pop a task from the Redis list (blocking right pop with timeout)
      const result = await redis.brpop(queueKey, 30); // 30 second timeout

      if (result && result[1]) {
        const jobData: WatermarkJobData = JSON.parse(result[1]);
        logger.info(
          `🔄 Processing job for collection: ${jobData.collectionId}`
        );

        const processingResult = await processWatermarkJob(jobData);

        if (processingResult.success) {
          logger.info(
            `✅ Job completed successfully for collection: ${jobData.collectionId}`
          );
          consecutiveErrors = 0; // Reset error counter on success
        } else {
          logger.warn(
            `⚠️ Job skipped for collection: ${jobData.collectionId} - ${processingResult.reason}`
          );
        }
      } else {
        // No job received within timeout - this is normal
        logger.debug("No jobs in queue, continuing to poll...");
        consecutiveErrors = 0; // Reset error counter on normal timeout
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
        await new Promise((resolve) => setTimeout(resolve, retryDelay));
      } else {
        await new Promise((resolve) => setTimeout(resolve, baseRetryDelay));
      }
    }
  }
};

// Enhanced graceful shutdown with queue cleanup
const gracefulShutdown = async (signal: string) => {
  logger.info(`Received ${signal}, shutting down worker gracefully...`);

  try {
    // Get current processing status and log
    const queueLength = await redis.llen("watermark-processing");
    logger.info(`Queue length at shutdown: ${queueLength}`);

    await redis.quit();
    logger.info("Redis connection closed");
  } catch (error) {
    logger.error("Error during graceful shutdown:", error);
  }

  process.exit(0);
};

// Start the worker
startWorker().catch((err) => {
  logger.error("Worker failed to start:", err);
  process.exit(1);
});

// Graceful shutdown handlers
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

// Handle uncaught exceptions
process.on("uncaughtException", (error) => {
  logger.error("Uncaught Exception:", error);
  process.exit(1);
});

process.on("unhandledRejection", (reason, promise) => {
  logger.error("Unhandled Rejection at:", promise, "reason:", reason);
  process.exit(1);
});

export default startWorker;
