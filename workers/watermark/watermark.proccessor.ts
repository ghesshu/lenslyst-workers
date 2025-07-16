import sharp from "sharp";
import { createCanvas } from "canvas";
import { WatermarkConfig } from "../../models/collection.model";

interface ProcessImageOptions {
  inputBuffer: Buffer;
  watermarkConfig: WatermarkConfig;
  watermarkImageBuffer?: Buffer;
}

async function processImageWithWatermark({
  inputBuffer,
  watermarkConfig,
  watermarkImageBuffer,
}: ProcessImageOptions): Promise<Buffer> {
  const image = sharp(inputBuffer);
  const metadata = await image.metadata();

  if (!metadata.width || !metadata.height) {
    throw new Error("Unable to read image dimensions.");
  }

  const { width, height } = metadata;

  // Convert normalized size (0–1) to actual pixel width
  const pixelSize = Math.min(
    Math.round(watermarkConfig.size * width),
    Math.floor(width * 0.08)
  );

  if (watermarkConfig.previewMode === "none") {
    return image.toBuffer();
  }

  if (watermarkConfig.previewMode === "blurred") {
    return image.blur(20).toBuffer();
  }

  const wc = watermarkConfig.text as string;

  if (watermarkConfig.previewMode === "watermarked") {
    let watermarkBuffer: Buffer;
    let wmWidth: number;
    let wmHeight: number;

    if (watermarkConfig.type === "text") {
      const canvas = createCanvas(pixelSize * 10, pixelSize * 2);
      const ctx = canvas.getContext("2d");

      ctx.font = `${pixelSize}px Arial`;
      const textWidth = ctx.measureText(wc).width;
      const textHeight = pixelSize * 1.2;

      const textCanvas = createCanvas(textWidth, textHeight);
      const textCtx = textCanvas.getContext("2d");

      textCtx.translate(textWidth / 2, textHeight / 2);
      textCtx.rotate((watermarkConfig.rotation * Math.PI) / 180);
      textCtx.translate(-textWidth / 2, -textHeight / 2);

      textCtx.font = `${pixelSize}px Arial`;
      textCtx.fillStyle = `rgba(255, 255, 255, ${watermarkConfig.opacity})`;
      textCtx.textBaseline = "top";
      textCtx.fillText(wc, 0, 0);

      watermarkBuffer = textCanvas.toBuffer("image/png");
      wmWidth = textWidth;
      wmHeight = textHeight;
    } else {
      if (!watermarkImageBuffer) {
        throw new Error("Watermark image is required for type 'image'");
      }

      // Resize image watermark based on pixelSize (width)
      const resized = await sharp(watermarkImageBuffer)
        .resize({ width: pixelSize })
        .rotate(watermarkConfig.rotation)
        .toBuffer();

      const faded = await sharp(resized)
        .composite([
          {
            input: Buffer.from(
              `<svg><rect x="0" y="0" width="100%" height="100%" fill="white" fill-opacity="${watermarkConfig.opacity}"/></svg>`
            ),
            blend: "dest-in",
          },
        ])
        .toBuffer();

      const wmMeta = await sharp(faded).metadata();
      watermarkBuffer = faded;
      wmWidth = wmMeta.width ?? pixelSize;
      wmHeight = wmMeta.height ?? pixelSize;
    }

    // Position watermark (single only)
    const pos = calculatePosition(
      watermarkConfig.alignment,
      width,
      height,
      wmWidth,
      wmHeight
    );

    const overlays = generateOverlayPattern(
      watermarkConfig.tileType,
      pos,
      width,
      height,
      watermarkBuffer,
      wmWidth,
      wmHeight
    );

    return image.composite(overlays).toBuffer();
  }

  throw new Error("Unsupported preview mode.");
}

function calculatePosition(
  alignment: string,
  imageWidth: number,
  imageHeight: number,
  wmWidth: number,
  wmHeight: number
): { x: number; y: number } {
  const padding = 20;

  const map = {
    northwest: { x: padding, y: padding },
    north: { x: (imageWidth - wmWidth) / 2, y: padding },
    northeast: { x: imageWidth - wmWidth - padding, y: padding },
    west: { x: padding, y: (imageHeight - wmHeight) / 2 },
    center: { x: (imageWidth - wmWidth) / 2, y: (imageHeight - wmHeight) / 2 },
    east: {
      x: imageWidth - wmWidth - padding,
      y: (imageHeight - wmHeight) / 2,
    },
    southwest: { x: padding, y: imageHeight - wmHeight - padding },
    south: {
      x: (imageWidth - wmWidth) / 2,
      y: imageHeight - wmHeight - padding,
    },
    southeast: {
      x: imageWidth - wmWidth - padding,
      y: imageHeight - wmHeight - padding,
    },
  };

  const pos = map[alignment as keyof typeof map];
  if (!pos) throw new Error("Invalid alignment");

  return {
    x: Math.round(pos.x),
    y: Math.round(pos.y),
  };
}

function generateOverlayPattern(
  tileType: "single" | "grid" | "diagonal",
  startPos: { x: number; y: number },
  imgWidth: number,
  imgHeight: number,
  watermark: Buffer,
  wmWidth: number,
  wmHeight: number
): sharp.OverlayOptions[] {
  switch (tileType) {
    case "single":
      return [{ input: watermark, top: startPos.y, left: startPos.x }];
    case "grid":
      return generateGrid(imgWidth, imgHeight, wmWidth, wmHeight, watermark);
    case "diagonal":
      return generateDiagonal(
        imgWidth,
        imgHeight,
        wmWidth,
        wmHeight,
        watermark
      );
    default:
      throw new Error("Invalid tileType");
  }
}

function generateGrid(
  imgWidth: number,
  imgHeight: number,
  wmWidth: number,
  wmHeight: number,
  watermark: Buffer
): sharp.OverlayOptions[] {
  const overlays: sharp.OverlayOptions[] = [];
  const spacingX = wmWidth + 40;
  const spacingY = wmHeight + 40;

  for (let y = 0; y + wmHeight <= imgHeight; y += spacingY) {
    for (let x = 0; x + wmWidth <= imgWidth; x += spacingX) {
      overlays.push({ input: watermark, top: y, left: x });
    }
  }

  return overlays;
}

function generateDiagonal(
  imgWidth: number,
  imgHeight: number,
  wmWidth: number,
  wmHeight: number,
  watermark: Buffer
): sharp.OverlayOptions[] {
  const overlays: sharp.OverlayOptions[] = [];
  const spacing = Math.max(wmWidth, wmHeight) + 60;

  for (let i = -imgHeight; i < imgWidth * 2; i += spacing) {
    const x = i;
    const y = i;
    if (x + wmWidth <= imgWidth && y + wmHeight <= imgHeight) {
      overlays.push({ input: watermark, top: y, left: x });
    }
  }

  return overlays;
}

export { processImageWithWatermark };
