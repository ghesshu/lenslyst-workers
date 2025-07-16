import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/cloudfront-signer";

export const allowedMimeTypes = [
  "image/jpeg",
  "image/png",
  "image/gif",
  "image/webp",
];

export const s3Client = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
  },
});

export const uploadToS3 = async (
  bucketName: string,
  fileKey: string,
  fileBuffer: Buffer,
  mimeType: string
) => {
  const fileExtension = mimeType.split("/")[1];
  const command = new PutObjectCommand({
    Bucket: bucketName,
    Key: fileKey,
    Body: fileBuffer,
    ContentType: mimeType,
    ContentDisposition: `attachment; filename="${fileKey}.${fileExtension}"`,
  });

  await s3Client.send(command);
  return fileKey;
};

// export const generateSignedUrl = (key: string) => {
//   return getSignedUrl({
//     url: `${process.env.CLOUDFRONT_DOMAIN}/${key}`,
//     dateLessThan: new Date(Date.now() + 1000 * 60 * 60 * 24).toISOString(),
//     keyPairId: process.env.CLOUDFRONT_KEY_PAIR_ID!,
//     privateKey: process.env.CLOUDFRONT_PRIVATE_KEY!,
//   });
// };
