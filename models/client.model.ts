import mongoose, { Schema, Document } from "mongoose";

interface IClient extends Document {
  name: string;
  email: string;
  projects: mongoose.Schema.Types.ObjectId[];
}

const clientSchema = new Schema<IClient>({
  name: { type: String, required: true },
  email: { type: String, required: true, unique: true },
  projects: [{ type: Schema.Types.ObjectId, ref: "Project" }],
});

export const Client = mongoose.model<IClient>("Client", clientSchema);
