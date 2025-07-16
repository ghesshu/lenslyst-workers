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

// Function to update watermark progress
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

    const updatedProgress = { ...currentProgress, ...progress };
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

// Function to process a watermark job
const processWatermarkJob = async (jobData: WatermarkJobData) => {
  try {
    logger.info(
      "Processing watermark job for collection:",
      jobData.collectionId
    );
    logger.info("Job data:", JSON.stringify(jobData, null, 2));

    const { collectionId, watermarkConfig, slug } = jobData;

    logger.info(`Processing watermark for collection: ${collectionId}`);
    logger.info(`Watermark type: ${watermarkConfig.type}`);

    if (watermarkConfig.imageKey) {
      logger.info(`Image key: ${watermarkConfig.imageKey}`);
    }

    // Get all images for this collection
    const images = await File.find({ collectionSlug: slug });
    logger.info(`Found ${images.length} images to process`);

    // Check if there's existing watermarked content and delete it
    const collection = await Collection.findById(collectionId);
    if (collection?.watermarkProgress?.status === "completed") {
      logger.info("Deleting existing watermarked directory...");
      await deleteWatermarkedDirectory(collectionId);
    }

    // Initialize progress
    await updateWatermarkProgress(collectionId, {
      total: images.length,
      watermarked: 0,
      locked: true,
      status: "processing",
    });

    // Get watermark image if type is image
    let watermarkImageBuffer: Buffer | undefined;

    if (watermarkConfig.type === "image" && watermarkConfig.imageKey) {
      watermarkImageBuffer = await getImageFromS3(watermarkConfig.imageKey);
    }

    // Process each image
    for (let i = 0; i < images.length; i++) {
      const image = images[i];
      logger.info(`Processing image ${i + 1}/${images.length}: ${image.key}`);

      try {
        // Get image from S3
        const imageBuffer = await getImageFromS3(image.key);

        // Process image with watermark
        const processedBuffer = await processImageWithWatermark({
          inputBuffer: imageBuffer,
          watermarkConfig,
          watermarkImageBuffer,
        });

        // Generate processed image key
        const originalFileName = image.key.split("/").pop() || `image-${i}`;
        const processedImageKey = `watermarked-${collectionId}/${originalFileName}`;

        // Upload processed image to S3
        await uploadProcessedImage(processedBuffer, processedImageKey);

        // Create watermarked file entry
        await WatermarkedFile.create({
          name: originalFileName,
          key: processedImageKey,
          type: image.type,
          size: processedBuffer.length,
          collectionId,
          originalFileId: image._id,
          workspaceId: image.workspaceId,
        });

        // Update progress
        await updateWatermarkProgress(collectionId, {
          watermarked: i + 1,
        });

        logger.info(`✅ Processed image ${i + 1}/${images.length}`);
      } catch (error) {
        logger.error(`❌ Error processing image ${image.key}:`, error);

        // Update progress to failed status
        await updateWatermarkProgress(collectionId, {
          status: "failed",
          locked: false,
        });

        throw error;
      }
    }

    // Mark as completed
    await updateWatermarkProgress(collectionId, {
      status: "completed",
      locked: false,
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

    // Update progress to failed status
    try {
      await updateWatermarkProgress(jobData.collectionId, {
        status: "failed",
        locked: false,
      });
    } catch (progressError) {
      logger.error("Error updating progress to failed:", progressError);
    }

    throw error;
  }
};

// Worker function to poll and process tasks from Redis
const startWorker = async () => {
  const queueKey = "watermark-processing";
  logger.info("Starting Redis worker to poll tasks...");

  while (true) {
    try {
      // Pop a task from the Redis list (blocking right pop)
      const jobDataString = await redis.brpop(queueKey, 0); // 0 means wait indefinitely
      if (jobDataString && jobDataString[1]) {
        const jobData: WatermarkJobData = JSON.parse(jobDataString[1]);
        await processWatermarkJob(jobData);
      }
    } catch (error) {
      logger.error("Error processing task from Redis:", error);
      // Optional: Add failed task to a dead-letter queue or log for retry
      await new Promise((resolve) => setTimeout(resolve, 1000)); // Wait before retrying
    }
  }
};

// Start the worker
startWorker().catch((err) => {
  logger.error("Worker failed to start:", err);
  process.exit(1);
});

// Graceful shutdown
process.on("SIGTERM", async () => {
  logger.info("Received SIGTERM, shutting down worker gracefully...");
  await redis.quit();
  process.exit(0);
});

process.on("SIGINT", async () => {
  logger.info("Received SIGINT, shutting down worker gracefully...");
  await redis.quit();
  process.exit(0);
});

export default startWorker;
