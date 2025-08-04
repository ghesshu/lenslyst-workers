import mongoose, { Schema, Document } from "mongoose";

export type WatermarkConfig = {
  requirePayment?: boolean;
  amount?: number;
  type: "text" | "image";
  text?: string;
  imageKey?: string;
  alignment:
    | "northwest"
    | "north"
    | "northeast"
    | "west"
    | "center"
    | "east"
    | "southwest"
    | "south"
    | "southeast";
  opacity: number;
  size: number;
  rotation: number;
  tileType: "single" | "grid" | "diagonal";
  textColor?: string;
  previewMode: "watermarked" | "blurred" | "none";
  CreatedAt: Date;
  UpdatedAt: Date;
};

export type WatermarkProgress = {
  total: number;
  watermarked: number;
  locked: boolean;
  status: "idle" | "queued" | "processing" | "completed" | "failed";
  queuedAt?: Date;
  startedAt?: Date;
  completedAt?: Date;
  estimatedTimeRemaining?: number; // in seconds
  currentImageName?: string;
};

// Enhanced progress response type for API responses
export type WatermarkProgressResponse = {
  collectionId: string;
  collectionName: string;
  status: "idle" | "queued" | "processing" | "completed" | "failed";
  totalImages: number;
  processedImages: number;
  queuePosition?: number;
  queuedAt?: Date;
  startedAt?: Date;
  completedAt?: Date;
  estimatedTimeRemaining?: number;
  currentImageName?: string;
  progressPercentage: number;
  elapsedTime?: number; // in seconds
};

interface ICollection extends Document {
  name: string;
  slug: string;
  url: string;
  description: string;
  creatorId: mongoose.Schema.Types.ObjectId;
  workspaceId: mongoose.Schema.Types.ObjectId;
  noOfFiles: number;
  isPublished: boolean;
  coverPhotoKey: string;
  template: string;
  createdAt: Date;
  watermarkConfig?: WatermarkConfig;
  watermarkProgress?: WatermarkProgress;
  setWatermarkConfig(config: WatermarkConfig): void;
  setWatermarkProgress(progress: WatermarkProgress): void;
  canBeQueued(): boolean;
  isInProgress(): boolean;
  isWatermarkProgressLocked(): boolean;
  lockWatermarkProgress(): void;
  unlockWatermarkProgress(): void;
}

const collectionSchema = new Schema<ICollection>({
  name: { type: String, required: true },
  slug: { type: String, required: true },
  description: { type: String },
  creatorId: { type: Schema.Types.ObjectId, ref: "User", required: true },
  workspaceId: {
    type: Schema.Types.ObjectId,
    ref: "Workspace",
    required: true,
  },
  coverPhotoKey: {
    type: String,
  },
  noOfFiles: { type: Number, default: 0 },
  isPublished: { type: Boolean, default: false },
  url: { type: String },
  template: {
    type: String,
    enum: ["classic", "lumen", "zest", "muse"],
    default: "classic",
  },
  createdAt: { type: Date, default: Date.now },
  watermarkConfig: {
    requirePayment: { type: Boolean, default: false },
    amount: { type: Number, default: 0 },
    type: {
      type: String,
      enum: ["text", "image"],
      required: true,
    },
    text: { type: String },
    imageKey: { type: String },
    alignment: {
      type: String,
      enum: [
        "northwest",
        "north",
        "northeast",
        "west",
        "center",
        "east",
        "southwest",
        "south",
        "southeast",
      ],
    },
    opacity: { type: Number, default: 0.6, min: 0, max: 1 },
    size: { type: Number, default: 0.3, min: 0, max: 1 },
    rotation: { type: Number, default: 0, min: 0, max: 360 },
    tileType: {
      type: String,
      enum: ["single", "grid", "diagonal"],
      default: "single",
    },
    textColor: { type: String },
    previewMode: {
      type: String,
      enum: ["watermarked", "blurred", "none"],
      default: "none",
    },
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now },
  },
  watermarkProgress: {
    total: { type: Number, default: 0 },
    watermarked: { type: Number, default: 0 },
    locked: { type: Boolean, default: false },
    status: {
      type: String,
      enum: ["idle", "queued", "processing", "completed", "failed"],
      default: "idle",
    },
    queuedAt: { type: Date },
    startedAt: { type: Date },
    completedAt: { type: Date },
    estimatedTimeRemaining: { type: Number },
    currentImageName: { type: String },
  },
});

collectionSchema.methods.setWatermarkConfig = function (
  config: WatermarkConfig
) {
  this.watermarkConfig = {
    ...config,
    CreatedAt: config.CreatedAt || new Date(),
    UpdatedAt: new Date(),
  };
};

collectionSchema.methods.setWatermarkProgress = function (
  progress: WatermarkProgress
) {
  this.watermarkProgress = progress;
};

// Method to check if collection can be queued
collectionSchema.methods.canBeQueued = function (): boolean {
  const status = this.watermarkProgress?.status;
  return (
    !status ||
    status === "idle" ||
    status === "completed" ||
    status === "failed"
  );
};

// Method to check if collection is currently in progress (queued or processing)
collectionSchema.methods.isInProgress = function (): boolean {
  const status = this.watermarkProgress?.status;
  return status === "queued" || status === "processing";
};

// Method to check if watermark progress is locked
collectionSchema.methods.isWatermarkProgressLocked = function (): boolean {
  return this.watermarkProgress?.locked || false;
};

// Method to lock watermark progress for this collection
collectionSchema.methods.lockWatermarkProgress = function (): void {
  if (!this.watermarkProgress) {
    this.watermarkProgress = {
      total: 0,
      watermarked: 0,
      locked: true,
      status: "idle",
    };
  } else {
    this.watermarkProgress.locked = true;
  }
};

// Method to unlock watermark progress for this collection
collectionSchema.methods.unlockWatermarkProgress = function (): void {
  if (!this.watermarkProgress) {
    this.watermarkProgress = {
      total: 0,
      watermarked: 0,
      locked: false,
      status: "idle",
    };
  } else {
    this.watermarkProgress.locked = false;
  }
};

collectionSchema.index({ slug: 1, workspaceId: 1 }, { unique: true });

export const Collection = mongoose.model<ICollection>(
  "Collection",
  collectionSchema
);
