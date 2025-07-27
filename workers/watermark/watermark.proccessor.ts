import sharp from "sharp";
import { createCanvas, registerFont } from "canvas";
import { WatermarkConfig } from "../../models/collection.model";


interface ProcessImageOptions {
  inputBuffer: Buffer;
  watermarkConfig: WatermarkConfig;
  watermarkImageBuffer?: Buffer;
  fontPath?: string; // Optional custom font
}

interface WatermarkDimensions {
  width: number;
  height: number;
  buffer: Buffer;
}

// Cache for processed watermarks to improve performance
const watermarkCache = new Map<string, WatermarkDimensions>();

async function processImageWithWatermark({
  inputBuffer,
  watermarkConfig,
  watermarkImageBuffer,
  fontPath,
}: ProcessImageOptions): Promise<Buffer> {
  const image = sharp(inputBuffer);
  const metadata = await image.metadata();

  if (!metadata.width || !metadata.height) {
    throw new Error("Unable to read image dimensions.");
  }

  const { width, height, format } = metadata;

  // Early return for no watermark mode
  if (watermarkConfig.previewMode === "none") {
    return image.toBuffer();
  }

  // Blur mode with adaptive blur based on image size
  if (watermarkConfig.previewMode === "blurred") {
    const blurAmount = Math.min(
      50,
      Math.max(10, Math.round(Math.min(width, height) * 0.02))
    );
    return image.blur(blurAmount).toBuffer();
  }

  // Watermark mode
  if (watermarkConfig.previewMode === "watermarked") {
    // Generate cache key for this watermark configuration
    const cacheKey = generateCacheKey(watermarkConfig, width, height);

    let watermarkData: WatermarkDimensions;

    // Check cache first
    if (watermarkCache.has(cacheKey) && watermarkConfig.type === "text") {
      watermarkData = watermarkCache.get(cacheKey)!;
    } else {
      watermarkData = await createWatermark({
        watermarkConfig,
        imageWidth: width,
        imageHeight: height,
        watermarkImageBuffer,
        fontPath,
      });

      // Cache text watermarks only
      if (watermarkConfig.type === "text") {
        watermarkCache.set(cacheKey, watermarkData);
      }
    }

    // Generate overlay pattern based on tile type
    const overlays = generateOverlayPattern(
      watermarkConfig.tileType,
      watermarkConfig.alignment,
      width,
      height,
      watermarkData.buffer,
      watermarkData.width,
      watermarkData.height
    );

    // Apply watermarks with optimized compositing
    const watermarkedImage = await image.composite(overlays);

    // Preserve original format if possible
    if (format === "jpeg" || format === "jpg") {
      return watermarkedImage.jpeg({ quality: 95 }).toBuffer();
    } else if (format === "png") {
      return watermarkedImage.png({ compressionLevel: 9 }).toBuffer();
    } else if (format === "webp") {
      return watermarkedImage.webp({ quality: 95 }).toBuffer();
    } else {
      return watermarkedImage.toBuffer();
    }
  }

  throw new Error("Unsupported preview mode.");
}

async function createWatermark({
  watermarkConfig,
  imageWidth,
  imageHeight,
  watermarkImageBuffer,
  fontPath,
}: {
  watermarkConfig: WatermarkConfig;
  imageWidth: number;
  imageHeight: number;
  watermarkImageBuffer?: Buffer;
  fontPath?: string;
}): Promise<WatermarkDimensions> {
  // Calculate adaptive watermark size
  const { pixelSize, maxWidth, maxHeight } = calculateAdaptiveSize(
    watermarkConfig.size,
    imageWidth,
    imageHeight
  );

  if (watermarkConfig.type === "text") {
    return createTextWatermark({
      text: watermarkConfig.text || "Watermark",
      fontSize: pixelSize,
      rotation: watermarkConfig.rotation,
      opacity: watermarkConfig.opacity,
      textColor: watermarkConfig.textColor || "#FFFFFF",
      maxWidth,
      maxHeight,
      fontPath,
      tileType: watermarkConfig.tileType,
      imageWidth,
      imageHeight,
    });
  } else {
    return createImageWatermark({
      watermarkImageBuffer: watermarkImageBuffer!,
      targetSize: pixelSize,
      rotation: watermarkConfig.rotation,
      opacity: watermarkConfig.opacity,
      maxWidth,
      maxHeight,
      tileType: watermarkConfig.tileType,
      imageWidth,
      imageHeight,
    });
  }
}

function calculateAdaptiveSize(
  normalizedSize: number,
  imageWidth: number,
  imageHeight: number
): { pixelSize: number; maxWidth: number; maxHeight: number } {
  // Use geometric mean for better scaling across different aspect ratios
  const baseDimension = Math.sqrt(imageWidth * imageHeight);

  // Progressive scaling: smaller watermarks for smaller images
  const scaleFactor = Math.min(1, baseDimension / 1000) * 0.3;

  // Convert normalized size (0-1) to actual pixels
  const pixelSize = Math.max(
    16, // Minimum size
    Math.round(normalizedSize * baseDimension * scaleFactor)
  );

  // Set maximum dimensions
  const maxWidth = Math.round(imageWidth * 0.4);
  const maxHeight = Math.round(imageHeight * 0.4);

  return { pixelSize, maxWidth, maxHeight };
}

async function createTextWatermark({
  text,
  fontSize,
  rotation,
  opacity,
  textColor,
  maxWidth,
  maxHeight,
  fontPath,
  tileType,
  imageWidth,
  imageHeight,
}: {
  text: string;
  fontSize: number;
  rotation: number;
  opacity: number;
  textColor: string;
  maxWidth: number;
  maxHeight: number;
  fontPath?: string;
  tileType?: "single" | "grid" | "diagonal";
  imageWidth?: number;
  imageHeight?: number;
}): Promise<WatermarkDimensions> {
  // Register custom font if provided
  if (fontPath) {
    try {
      registerFont(fontPath, { family: "CustomFont" });
    } catch (error) {
      console.warn("Failed to load custom font, using default", error);
    }
  }

  // Create measurement canvas
  const measureCanvas = createCanvas(1, 1);
  const measureCtx = measureCanvas.getContext("2d");

  // Start with the requested font size
  let adjustedFontSize = fontSize;
  const fontFamily = fontPath ? "CustomFont" : "Arial";

  // Measure and adjust font size if text is too large
  let textWidth: number;
  let attempts = 0;
  const maxAttempts = 10;

  do {
    measureCtx.font = `bold ${adjustedFontSize}px ${fontFamily}`;
    const metrics = measureCtx.measureText(text);
    textWidth = metrics.width;

    // If text is too wide even without rotation, reduce font size
    if (textWidth > maxWidth * 0.9 && attempts < maxAttempts) {
      adjustedFontSize = Math.floor(adjustedFontSize * 0.85);
      attempts++;
    } else {
      break;
    }
  } while (textWidth > maxWidth * 0.9 && attempts < maxAttempts);

  // Grid and diagonal tiling logic: ensure watermark can be divided at least 4 times
  if (
    (tileType === "grid" || tileType === "diagonal") &&
    imageWidth &&
    imageHeight
  ) {
    let gridAttempts = 0;
    const maxGridAttempts = 15;

    while (gridAttempts < maxGridAttempts) {
      // Recalculate text dimensions with current font size
      measureCtx.font = `bold ${adjustedFontSize}px ${fontFamily}`;
      const metrics = measureCtx.measureText(text);
      const currentTextWidth = metrics.width;
      const currentTextHeight = adjustedFontSize * 1.2;

      // Calculate how many times the watermark fits in the image
      const widthDivisions = Math.floor(imageWidth / currentTextWidth);
      const heightDivisions = Math.floor(imageHeight / currentTextHeight);

      // Check if both divisions are 4 or more
      if (widthDivisions >= 4 && heightDivisions >= 4) {
        textWidth = currentTextWidth;
        break;
      }

      // Reduce font size and try again
      adjustedFontSize = Math.floor(adjustedFontSize * 0.9);
      gridAttempts++;

      // Prevent font size from becoming too small
      if (adjustedFontSize < 8) {
        adjustedFontSize = 8;
        measureCtx.font = `bold ${adjustedFontSize}px ${fontFamily}`;
        const finalMetrics = measureCtx.measureText(text);
        textWidth = finalMetrics.width;
        break;
      }
    }
  }

  // Rest of the function remains the same, but use adjustedFontSize
  const textHeight = adjustedFontSize * 1.2;

  // Calculate canvas size with padding for rotation
  const diagonal = Math.sqrt(textWidth * textWidth + textHeight * textHeight);
  const canvasSize = Math.ceil(
    Math.min(diagonal * 1.5, Math.max(maxWidth, maxHeight) * 1.5)
  );

  // Create final canvas
  const canvas = createCanvas(canvasSize, canvasSize);
  const ctx = canvas.getContext("2d");

  // Enable anti-aliasing
  ctx.antialias = "subpixel";
  ctx.patternQuality = "best";
  ctx.quality = "best";

  // Clear canvas
  ctx.clearRect(0, 0, canvasSize, canvasSize);

  // Set up text properties with adjusted font size
  ctx.font = `bold ${adjustedFontSize}px ${fontFamily}`;
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";

  // Apply transformations
  ctx.save();
  ctx.translate(canvasSize / 2, canvasSize / 2);
  ctx.rotate((rotation * Math.PI) / 180);

  // Parse color and apply opacity
  const color = parseColor(textColor);
  ctx.fillStyle = color;
  ctx.globalAlpha = opacity;

  // Draw text
  ctx.fillText(text, 0, 0);

  ctx.restore();

  // Convert to buffer and trim
  const buffer = canvas.toBuffer("image/png");

  // Trim transparent pixels
  const trimmed = await sharp(buffer).trim({ threshold: 1 }).toBuffer();
  const trimmedMeta = await sharp(trimmed).metadata();

  // Final safety check - resize if still too large
  let finalBuffer = trimmed;
  let finalWidth = trimmedMeta.width || canvasSize;
  let finalHeight = trimmedMeta.height || canvasSize;

  if (finalWidth > maxWidth || finalHeight > maxHeight) {
    finalBuffer = await sharp(trimmed)
      .resize({
        width: maxWidth,
        height: maxHeight,
        fit: "inside",
        kernel: "lanczos3",
      })
      .toBuffer();

    const resizedMeta = await sharp(finalBuffer).metadata();
    finalWidth = resizedMeta.width || maxWidth;
    finalHeight = resizedMeta.height || maxHeight;
  }

  return {
    width: finalWidth,
    height: finalHeight,
    buffer: finalBuffer,
  };
}

async function createImageWatermark({
  watermarkImageBuffer,
  targetSize,
  rotation,
  opacity,
  maxWidth,
  maxHeight,
  tileType,
  imageWidth,
  imageHeight,
}: {
  watermarkImageBuffer: Buffer;
  targetSize: number;
  rotation: number;
  opacity: number;
  maxWidth: number;
  maxHeight: number;
  tileType?: "single" | "grid" | "diagonal";
  imageWidth?: number;
  imageHeight?: number;
}): Promise<WatermarkDimensions> {
  // Get original watermark dimensions
  const originalMeta = await sharp(watermarkImageBuffer).metadata();
  if (!originalMeta.width || !originalMeta.height) {
    throw new Error("Unable to read watermark image dimensions");
  }

  // Calculate scaled dimensions maintaining aspect ratio
  const aspectRatio = originalMeta.width / originalMeta.height;
  let targetWidth = targetSize;
  let targetHeight = targetSize / aspectRatio;

  // Adjust if height is larger
  if (targetHeight > targetSize) {
    targetHeight = targetSize;
    targetWidth = targetSize * aspectRatio;
  }

  // Apply maximum constraints (similar to text watermark size adjustment)
  let attempts = 0;
  const maxAttempts = 10;

  while (
    (targetWidth > maxWidth * 0.9 || targetHeight > maxHeight * 0.9) &&
    attempts < maxAttempts
  ) {
    targetWidth = Math.floor(targetWidth * 0.85);
    targetHeight = Math.floor(targetHeight * 0.85);
    attempts++;
  }

  // Grid and diagonal tiling logic: ensure watermark can be divided at least 4 times
  if (
    (tileType === "grid" || tileType === "diagonal") &&
    imageWidth &&
    imageHeight
  ) {
    let gridAttempts = 0;
    const maxGridAttempts = 15;

    while (gridAttempts < maxGridAttempts) {
      // Calculate how many times the watermark fits in the image
      const widthDivisions = Math.floor(imageWidth / targetWidth);
      const heightDivisions = Math.floor(imageHeight / targetHeight);

      // Check if both divisions are 4 or more
      if (widthDivisions >= 4 && heightDivisions >= 4) {
        break;
      }

      // Reduce watermark size and try again
      targetWidth = Math.floor(targetWidth * 0.9);
      targetHeight = Math.floor(targetHeight * 0.9);
      gridAttempts++;

      // Prevent watermark from becoming too small
      if (targetWidth < 20 || targetHeight < 20) {
        targetWidth = Math.max(20, targetWidth);
        targetHeight = Math.max(20, targetHeight);
        break;
      }
    }
  }

  // Apply final maximum constraints
  if (targetWidth > maxWidth) {
    targetWidth = maxWidth;
    targetHeight = maxWidth / aspectRatio;
  }
  if (targetHeight > maxHeight) {
    targetHeight = maxHeight;
    targetWidth = maxHeight * aspectRatio;
  }

  // Process watermark with high quality
  let processed = sharp(watermarkImageBuffer).resize({
    width: Math.round(targetWidth),
    height: Math.round(targetHeight),
    fit: "inside",
    kernel: "lanczos3", // High quality scaling
  });

  // Apply rotation if needed
  if (rotation !== 0) {
    processed = processed.rotate(rotation, {
      background: { r: 0, g: 0, b: 0, alpha: 0 },
    });
  }

  // Replace the opacity application section (lines 456-467) with:
  const finalBuffer = await processed.toBuffer();

  // Apply opacity by creating a new image with the desired alpha channel
  const withOpacity = await sharp(finalBuffer)
    .ensureAlpha()
    .raw()
    .toBuffer({ resolveWithObject: true });

  // Modify alpha channel
  const { data, info } = withOpacity;
  for (let i = 3; i < data.length; i += 4) {
    data[i] = Math.round(data[i] * opacity);
  }

  const finalProcessed = await sharp(data, {
    raw: {
      width: info.width,
      height: info.height,
      channels: info.channels,
    },
  })
    .png()
    .toBuffer();

  const finalMeta = await sharp(finalProcessed).metadata();

  return {
    width: finalMeta.width || targetWidth,
    height: finalMeta.height || targetHeight,
    buffer: finalProcessed,
  };
}

function calculatePosition(
  alignment: string,
  imageWidth: number,
  imageHeight: number,
  wmWidth: number,
  wmHeight: number
): { x: number; y: number } {
  // Adaptive padding based on image size
  const paddingPercentage = Math.max(
    0.02,
    Math.min(0.05, 50 / Math.min(imageWidth, imageHeight))
  );
  const paddingX = Math.round(imageWidth * paddingPercentage);
  const paddingY = Math.round(imageHeight * paddingPercentage);

  // Ensure watermark stays within bounds
  const maxX = Math.max(0, imageWidth - wmWidth);
  const maxY = Math.max(0, imageHeight - wmHeight);

  const positions: Record<string, { x: number; y: number }> = {
    northwest: { x: paddingX, y: paddingY },
    north: { x: (imageWidth - wmWidth) / 2, y: paddingY },
    northeast: { x: maxX - paddingX, y: paddingY },
    west: { x: paddingX, y: (imageHeight - wmHeight) / 2 },
    center: { x: (imageWidth - wmWidth) / 2, y: (imageHeight - wmHeight) / 2 },
    east: { x: maxX - paddingX, y: (imageHeight - wmHeight) / 2 },
    southwest: { x: paddingX, y: maxY - paddingY },
    south: { x: (imageWidth - wmWidth) / 2, y: maxY - paddingY },
    southeast: { x: maxX - paddingX, y: maxY - paddingY },
  };

  const pos = positions[alignment];
  if (!pos) {
    throw new Error(`Invalid alignment: ${alignment}`);
  }

  return {
    x: Math.max(0, Math.min(maxX, Math.round(pos.x))),
    y: Math.max(0, Math.min(maxY, Math.round(pos.y))),
  };
}

function generateOverlayPattern(
  tileType: "single" | "grid" | "diagonal",
  alignment: string,
  imgWidth: number,
  imgHeight: number,
  watermark: Buffer,
  wmWidth: number,
  wmHeight: number
): sharp.OverlayOptions[] {
  // Validate watermark dimensions
  if (wmWidth > imgWidth || wmHeight > imgHeight) {
    console.warn("Watermark larger than image, scaling down");
    wmWidth = Math.min(wmWidth, imgWidth * 0.3);
    wmHeight = Math.min(wmHeight, imgHeight * 0.3);
  }

  switch (tileType) {
    case "single":
      const pos = calculatePosition(
        alignment,
        imgWidth,
        imgHeight,
        wmWidth,
        wmHeight
      );
      return [{ input: watermark, top: pos.y, left: pos.x }];

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
      throw new Error(`Invalid tileType: ${tileType}`);
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

  // Fixed 3x3 grid pattern
  const gridSize = 3;

  // Calculate spacing to fit exactly 3 watermarks per row/column
  const spacingX = Math.round(imgWidth / gridSize);
  const spacingY = Math.round(imgHeight / gridSize);

  // Center each watermark within its grid cell
  const offsetX = Math.round((spacingX - wmWidth) / 2);
  const offsetY = Math.round((spacingY - wmHeight) / 2);

  // Generate 3x3 grid
  for (let row = 0; row < gridSize; row++) {
    for (let col = 0; col < gridSize; col++) {
      // Calculate position for each grid cell
      const x = col * spacingX + offsetX;
      const y = row * spacingY + offsetY;

      // Ensure watermark stays within image bounds
      const finalX = Math.max(0, Math.min(imgWidth - wmWidth, x));
      const finalY = Math.max(0, Math.min(imgHeight - wmHeight, y));

      overlays.push({
        input: watermark,
        top: finalY,
        left: finalX,
      });
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

  // Define the diagonal pattern: 2-3-2 across 3 rows
  const rows = 3;
  const pattern = [2, 3, 2]; // watermarks per row

  // Calculate row spacing
  const rowSpacing = Math.round(imgHeight / rows);

  for (let row = 0; row < rows; row++) {
    const watermarksInRow = pattern[row];

    // Calculate column spacing for this row
    const colSpacing = Math.round(imgWidth / watermarksInRow);

    // Calculate vertical position for this row
    const y = row * rowSpacing + Math.round((rowSpacing - wmHeight) / 2);

    for (let col = 0; col < watermarksInRow; col++) {
      // Calculate horizontal position for this watermark
      const x = col * colSpacing + Math.round((colSpacing - wmWidth) / 2);

      // Ensure watermark stays within image bounds
      const finalX = Math.max(0, Math.min(imgWidth - wmWidth, x));
      const finalY = Math.max(0, Math.min(imgHeight - wmHeight, y));

      overlays.push({
        input: watermark,
        top: finalY,
        left: finalX,
      });
    }
  }

  return overlays;
}

function generateCacheKey(
  config: WatermarkConfig,
  width: number,
  height: number
): string {
  return `${config.type}_${config.text}_${config.size}_${config.opacity}_${config.rotation}_${config.textColor}_${width}_${height}`;
}

function parseColor(color: string): string {
  // Handle various color formats
  if (color.startsWith("#")) {
    return color;
  } else if (color.startsWith("rgb")) {
    return color;
  } else {
    // Named colors - add more as needed
    const namedColors: Record<string, string> = {
      white: "#FFFFFF",
      black: "#000000",
      red: "#FF0000",
      green: "#00FF00",
      blue: "#0000FF",
      yellow: "#FFFF00",
      // Add more named colors as needed
    };
    return namedColors[color.toLowerCase()] || "#FFFFFF";
  }
}

// Clear cache periodically to prevent memory leaks
setInterval(() => {
  if (watermarkCache.size > 100) {
    watermarkCache.clear();
  }
}, 3600000); // Clear every hour

export { processImageWithWatermark };
