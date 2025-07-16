import mongoose, { Schema, Document } from "mongoose";

interface User extends Document {
  fullName: string;
  email: string;
  profilePhoto: {
    name: string;
    key: string;
    size: number;
    type: string;
  } | null;
  password: string;
  role: string;
  isOnboarded: boolean;
  workspaces: { workspaceId: mongoose.Types.ObjectId; role: string }[];
  resetPasswordCode: string | null;
  resetPasswordCodeExpires: number | null;
}

export interface UserDocument extends User {
  createdAt: Date;
  updatedAt: Date;
  _doc?: any;
}

const userSchema = new Schema<User>({
  fullName: { type: String, required: true },
  email: { type: String, required: true, unique: true },
  profilePhoto: {
    name: { type: String, required: false },
    key: { type: String, required: false },
    size: { type: Number, required: false },
    type: { type: String, required: false },
  },
  password: { type: String, required: true },
  role: {
    type: String,
    enum: ["admin", "creator"],
    default: "creator",
  },
  isOnboarded: {
    type: Boolean,
    default: false,
  },
  workspaces: [
    {
      workspaceId: {
        type: mongoose.Types.ObjectId,
        ref: "Workspace",
        required: true,
      },
      role: {
        type: String,
        enum: ["admin", "editor", "viewer"],
        default: "editor",
      },
    },
  ],
  resetPasswordCode: { type: String },
  resetPasswordCodeExpires: { type: Number },
});

export const User = mongoose.model<User>("User", userSchema);
