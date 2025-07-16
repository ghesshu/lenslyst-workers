import { z } from "zod";

export const WatermarkConfigSchema = z.object({
  requirePayment: z.boolean().optional(),
  amount: z.number().min(0).optional(),
  type: z.enum(["text", "image"]),
  text: z.string().min(1).optional(),
  imageKey: z.string().min(1).optional(),
  alignment: z.enum([
    "northwest",
    "north",
    "northeast",
    "west",
    "center",
    "east",
    "southwest",
    "south",
    "southeast",
  ]),
  opacity: z.number().min(0).max(1),
  size: z.number().min(0.001).max(1),
  rotation: z.number().min(-360).max(360),
  tileType: z.enum(["single", "grid", "diagonal"]),
  textColor: z
    .string()
    .regex(/^#[0-9A-Fa-f]{6}$/)
    .optional(),
  previewMode: z.enum(["watermarked", "blurred", "none"]),
});

export type WatermarkConfigDTO = z.infer<typeof WatermarkConfigSchema>;
