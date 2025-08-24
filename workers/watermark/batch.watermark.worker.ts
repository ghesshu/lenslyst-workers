import { Collection } from "../../models/collection.model";
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
import { createHash } from "crypto";
import { WatermarkConfig, WatermarkProgress } from "../../types";

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

const getBatchOptions = (): BatchProcessingOptions => ({
  batchSize: parseInt(process.env.BATCH_SIZE || "5"),
  maxConcurrency: parseInt(
    process.env.MAX_CONCURRENCY || String(Math.max(1, cpus().length - 1))
  ),
  retryAttempts: parseInt(process.env.RETRY_ATTEMPTS || "3"),
  retryDelay: parseInt(process.env.RETRY_DELAY || "1000"),
});

const createChunks = <T>(items: T[], chunkSize: number): T[][] => {
  const chunks: T[][] = [];
  for (let i = 0; i < items.length; i += chunkSize) {
    chunks.push(items.slice(i, i + chunkSize));
  }
  return chunks;
};

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

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
      CacheControl: "max-age=31536000",
    });

    await s3Client.send(command);
    logger.info(`Uploaded processed image to: ${key}`);
  } catch (error) {
    logger.error(`Error uploading processed image with key ${key}:`, error);
    throw error;
  }
};

const deleteWatermarkedDirectory = async (
  collectionId: string
): Promise<void> => {
  try {
    const prefix = `watermark/watermarked-${collectionId}/`;

    const deletedDbRecords = await WatermarkedFile.deleteMany({ collectionId });
    logger.info(`Deleted ${deletedDbRecords.deletedCount} database records`);

    let continuationToken: string | undefined;
    let totalDeleted = 0;
    const batchSize = 100;

    do {
      const listCommand = new ListObjectsV2Command({
        Bucket: process.env.AWS_BUCKET_NAME!,
        Prefix: prefix,
        MaxKeys: 1000,
        ContinuationToken: continuationToken,
      });

      const listResponse = await s3Client.send(listCommand);
      continuationToken = listResponse.NextContinuationToken;

      if (listResponse.Contents && listResponse.Contents.length > 0) {
        logger.info(
          `Found ${listResponse.Contents.length} S3 objects to delete`
        );

        const objectsToDelete = listResponse.Contents.filter((obj) => obj.Key);
        const batches = createChunks(objectsToDelete, batchSize);

        for (const batch of batches) {
          const deletePromises = batch.map((obj) => {
            const deleteCommand = new DeleteObjectCommand({
              Bucket: process.env.AWS_BUCKET_NAME!,
              Key: obj.Key!,
            });
            return s3Client.send(deleteCommand).catch((error) => {
              logger.warn(`Failed to delete S3 object ${obj.Key}:`, error);
              return null;
            });
          });

          const results = await Promise.allSettled(deletePromises);
          const successful = results.filter(
            (result) => result.status === "fulfilled"
          ).length;
          totalDeleted += successful;

          logger.debug(`Deleted batch: ${successful}/${batch.length} objects`);

          if (batches.length > 1) {
            await delay(100);
          }
        }
      }
    } while (continuationToken);

    logger.info(
      `Successfully deleted watermarked directory: ${prefix} (${totalDeleted} S3 objects, ${deletedDbRecords.deletedCount} DB records)`
    );
  } catch (error) {
    logger.error(
      `Error deleting watermarked directory for collection ${collectionId}:`,
      error
    );
  }
};

const ensureCleanWatermarkDirectory = async (
  collectionId: string
): Promise<void> => {
  try {
    logger.info(
      `Ensuring clean watermark directory for collection: ${collectionId}`
    );

    await deleteWatermarkedDirectory(collectionId);

    const prefix = `watermark/watermarked-${collectionId}/`;
    const listCommand = new ListObjectsV2Command({
      Bucket: process.env.AWS_BUCKET_NAME!,
      Prefix: prefix,
      MaxKeys: 1,
    });

    const listResponse = await s3Client.send(listCommand);

    if (listResponse.Contents && listResponse.Contents.length > 0) {
      logger.warn(
        `Warning: ${listResponse.Contents.length} files still exist after cleanup attempt`
      );
    } else {
      logger.info(`Cleanup verification successful - no files remaining`);
    }
  } catch (error) {
    logger.error(
      `Error in ensureCleanWatermarkDirectory for collection ${collectionId}:`,
      error
    );
  }
};

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

const validateCollectionForProcessing = async (
  slug: string,
  collectionId?: string
): Promise<boolean> => {
  try {
    const collection = await Collection.findOne({ slug: slug });

    // Handle missing collection
    if (!collection) {
      logger.warn(`Collection ${slug} no longer exists, skipping processing`);

      try {
        await fetch(`${process.env.CANCEL_WATERMARK_URL}/${collectionId}`);
        logger.warn(`Collection ${slug} no longer exists, skipping processing`);
      } catch (cancelError) {
        logger.error(
          `Failed to cancel watermark for ${collectionId}:`,
          cancelError
        );
      }

      return false;
    }

    const status = collection.watermarkProgress?.status ?? "idle";

    // Skip if status is not active
    if (["idle", "completed"].includes(status)) {
      logger.warn(
        `Collection ${slug} status changed to ${status}, skipping processing`
      );
      return false;
    }

    return true;
  } catch (error) {
    logger.error(`Error validating collection ${slug}:`, error);
    return false;
  }
};

const processSingleImage = async (
  image: any,
  collectionId: string,
  watermarkConfig: WatermarkConfig,
  watermarkImageBuffer?: Buffer
): Promise<ImageProcessingResult> => {
  const startTime = Date.now();

  try {
    const imageBuffer = await getImageFromS3(image.key);

    const processedBuffer = await processImageWithWatermark({
      inputBuffer: imageBuffer,
      watermarkConfig,
      watermarkImageBuffer,
    });

    const originalFileName = image.key.split("/").pop() || `image-${image._id}`;
    const configHash = createHash("md5")
      .update(JSON.stringify(watermarkConfig))
      .digest("hex")
      .substring(0, 8);
    const processedImageKey = `watermark/watermarked-${collectionId}/${configHash}-${originalFileName}`;

    await uploadProcessedImage(
      processedBuffer,
      processedImageKey,
      image.type || "image/jpeg"
    );

    await WatermarkedFile.findOneAndUpdate(
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
        upsert: true,
        new: true,
      }
    );

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

const processImageWithRetry = async (
  image: any,
  collectionId: string,
  watermarkConfig: WatermarkConfig,
  watermarkImageBuffer: Buffer | undefined,
  options: BatchProcessingOptions
): Promise<ImageProcessingResult> => {
  let lastError: Error | undefined;

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

      if (attempt < options.retryAttempts - 1) {
        const backoffDelay = options.retryDelay * Math.pow(2, attempt);
        await delay(backoffDelay);
      }
    }
  }

  return {
    success: false,
    fileId: image._id,
    originalKey: image.key,
    error: lastError,
  };
};

const processBatch = async (
  images: any[],
  collectionId: string,
  watermarkConfig: WatermarkConfig,
  watermarkImageBuffer: Buffer | undefined,
  options: BatchProcessingOptions
): Promise<ImageProcessingResult[]> => {
  const chunks = createChunks(images, options.maxConcurrency);
  const results: ImageProcessingResult[] = [];

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

interface WatermarkJobResult {
  success: boolean;
  collectionId: string;
  reason?: string;
  skipped?: boolean;
  processedImages?: number;
  failedImages?: number;
  totalSize?: number;
  processingTime?: number;
  processedAt?: string;
  message?: string;
}

const processWatermarkJob = async (
  jobData: WatermarkJobData
): Promise<WatermarkJobResult> => {
  let hasStartedProcessing = false;
  const options = getBatchOptions();

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

    if (!(await validateCollectionForProcessing(slug, collectionId))) {
      return {
        success: false,
        collectionId,
        reason: "Collection no longer valid for processing",
        skipped: true,
      };
    }

    const images = await File.find({ collectionSlug: slug }).lean();
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

    logger.info("Cleaning up any existing watermarked content...");
    await ensureCleanWatermarkDirectory(collectionId);

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

    let watermarkImageBuffer: Buffer | undefined;
    if (watermarkConfig.type === "image" && watermarkConfig.imageKey) {
      logger.info("Downloading watermark image from S3...");
      watermarkImageBuffer = await getImageFromS3(watermarkConfig.imageKey);
    }

    const totalBatches = Math.ceil(images.length / options.batchSize);
    logger.info(
      `Processing ${images.length} images in ${totalBatches} batches`
    );

    for (let i = 0; i < images.length; i += options.batchSize) {
      const batchNumber = Math.floor(i / options.batchSize) + 1;
      const batch = images.slice(i, i + options.batchSize);

      logger.info(
        `Processing batch ${batchNumber}/${totalBatches} (${batch.length} images)`
      );

      if (!(await validateCollectionForProcessing(slug, collectionId))) {
        logger.warn(
          `Collection ${collectionId} no longer valid, stopping at batch ${batchNumber}`
        );
        break;
      }

      await updateWatermarkProgress(collectionId, {
        currentImageName: `Processing batch ${batchNumber}/${totalBatches}`,
        watermarked: metrics.processedCount,
      });

      const batchResults = await processBatch(
        batch,
        collectionId,
        watermarkConfig,
        watermarkImageBuffer,
        options
      );

      batchResults.forEach((result) => {
        if (result.success) {
          metrics.processedCount++;
          metrics.totalSize += result.size || 0;
        } else {
          metrics.failedCount++;
        }
      });

      const successfulInBatch = batchResults.filter((r) => r.success).length;
      await updateWatermarkProgress(collectionId, {
        watermarked: metrics.processedCount,
        currentImageName: `Completed batch ${batchNumber}/${totalBatches} (${successfulInBatch}/${batch.length} successful)`,
      });

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

    const totalTime = Date.now() - metrics.startTime;
    const averageTimePerImage = totalTime / images.length;
    const averageSizePerImage = metrics.totalSize / metrics.processedCount || 0;

    const finalStatus =
      metrics.failedCount === 0
        ? "completed"
        : metrics.processedCount === 0
        ? "failed"
        : "completed_with_errors";

    await updateWatermarkProgress(collectionId, {
      status: finalStatus as any,
      locked: false,
      completedAt: new Date(),
      currentImageName: undefined,
      estimatedTimeRemaining: 0,
      watermarked: metrics.processedCount,
    });

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

    return {
      success: false,
      collectionId: jobData.collectionId,
      reason: `Processing failed: ${(error as Error).message}`,
      skipped: false,
      processedImages: metrics.processedCount,
      failedImages: metrics.failedCount,
      totalSize: metrics.totalSize,
      processingTime: Date.now() - metrics.startTime,
    };
  }
};

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
  const baseRetryDelay = 1000;

  while (true) {
    try {
      const result = await redis.brpop(queueKey, 30);

      if (result && result[1]) {
        const jobData: WatermarkJobData = JSON.parse(result[1]);
        logger.info(
          `🔄 Processing JOB for collection: ${jobData.collectionId}`
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
          consecutiveErrors = 0;
        } else {
          logger.warn(
            `⚠️ Job skipped for collection: ${jobData.collectionId} - ${processingResult.reason}`
          );
        }
      } else {
        logger.debug("No jobs in queue, continuing to poll...");
        consecutiveErrors = 0;
      }
    } catch (error) {
      consecutiveErrors++;
      logger.error(
        `❌ Error processing task from Redis (${consecutiveErrors}/${maxConsecutiveErrors}):`,
        error
      );

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

const gracefulShutdown = async (signal: string) => {
  logger.info(`Received ${signal}, shutting down worker gracefully...`);

  try {
    const queueLength = await redis.llen("watermark-processing");
    logger.info(`Queue length at shutdown: ${queueLength}`);

    await redis.quit();
    logger.info("Redis connection closed");
  } catch (error) {
    logger.error("Error during graceful shutdown:", error);
  }

  process.exit(0);
};

startWorker().catch((err) => {
  logger.error("Worker failed to start:", err);
  process.exit(1);
});

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

process.on("uncaughtException", (error) => {
  logger.error("Uncaught Exception:", error);
  process.exit(1);
});

process.on("unhandledRejection", (reason, promise) => {
  logger.error("Unhandled Rejection at:", promise, "reason:", reason);
  process.exit(1);
});

export default startWorker;
