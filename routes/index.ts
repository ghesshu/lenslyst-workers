import { Request, Response, NextFunction } from "express";
import { Router } from "express";
import multer from "multer";
import { processImageWithWatermark } from "../workers/watermark/watermark.proccessor";
import logger from "../utils/logger";
import { WatermarkConfig } from "../types";


const router = Router();

const upload = multer({ storage: multer.memoryStorage() });

router.post(
  "/watermark",
  upload.fields([
    { name: "image", maxCount: 1 },
    { name: "watermarkImage", maxCount: 1 },
  ]),
  async (req: Request, res: Response): Promise<void> => {
    try {
      const imageFile = (req.files as any)?.["image"]?.[0];
      if (!imageFile) {
        res.status(400).json({ error: "Image file is required" });
        return;
      }

      const inputBuffer = imageFile.buffer;

      // Make watermarkImage optional - only required for image-type watermarks
      const watermarkImageBuffer = (req.files as any)?.["watermarkImage"]?.[0]
        ?.buffer;

      const watermarkConfigRaw = req.body.config;
      if (!watermarkConfigRaw) {
        res.status(400).json({ error: "Watermark config is required" });
        return;
      }

      const watermarkConfig: WatermarkConfig = JSON.parse(watermarkConfigRaw);

      // Validate that watermarkImage is provided when type is 'image'
      if (watermarkConfig.type === "image" && !watermarkImageBuffer) {
        res.status(400).json({
          error:
            "Watermark image file is required when watermark type is 'image'",
        });
        return;
      }

      const outputBuffer = await processImageWithWatermark({
        inputBuffer,
        watermarkConfig,
        watermarkImageBuffer, // This can now be undefined for text watermarks
      });

      res.set("Content-Type", "image/png");
      res.send(outputBuffer);
    } catch (error) {
      logger.error("Error processing image:", error);
      res.status(500).json({ error: "Failed to process image" });
    }
  }
);

export default router;
