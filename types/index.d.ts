interface UserPayload extends JwtPayload {
  id: string;
  role: {
    Admin: boolean;
    Creative: boolean;
    Client: boolean;
  };
}
export type Config = {
  visibility: "public" | "private" | "password";
  password?: string;
  requirePayment?: boolean;
  amount?: number;
}

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
