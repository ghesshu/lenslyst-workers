import mongoose, { Schema, Document } from "mongoose";

interface IWatermarkedFile extends Document {
  name: string;
  key: string;
  size: number;
  type: string;
  collectionId: mongoose.Schema.Types.ObjectId;
  originalFileId: mongoose.Schema.Types.ObjectId;
  workspaceId: mongoose.Schema.Types.ObjectId;
}

const watermarkedFileSchema = new Schema(
  {
    name: { type: String, required: true },
    key: { type: String, required: true },
    type: { type: String, required: true },
    size: { type: Number, required: true },
    collectionId: {
      type: Schema.Types.ObjectId,
      ref: "Collection",
      required: true,
    },
    originalFileId: {
      type: Schema.Types.ObjectId,
      ref: "File",
      required: true,
    },
    workspaceId: {
      type: Schema.Types.ObjectId,
      ref: "Workspace",
      required: true,
    },
  },
  { timestamps: true }
);

watermarkedFileSchema.index({ collectionId: 1, originalFileId: 1 });

export const WatermarkedFile = mongoose.model(
  "WatermarkedFile",
  watermarkedFileSchema
);
