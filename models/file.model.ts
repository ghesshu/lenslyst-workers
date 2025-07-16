import mongoose, { Schema, Document } from "mongoose";

interface IFile extends Document {
  name: string;
  key: string;
  size: number;
  type: string;
  collectionSlug: string;
  workspaceId: mongoose.Schema.Types.ObjectId;
}

const fileSchema = new Schema<IFile>(
  {
    name: { type: String, required: true },
    key: { type: String, required: true },
    type: { type: String, required: true },
    size: { type: Number, required: true },
    collectionSlug: { type: String, required: true },
    workspaceId: {
      type: Schema.Types.ObjectId,
      ref: "Workspace",
      required: true,
    },
  },
  { timestamps: true }
);

fileSchema.index({ collectionSlug: 1 });

export const File = mongoose.model<IFile>("File", fileSchema);
