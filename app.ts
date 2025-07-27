import express, { Express, ErrorRequestHandler } from "express";
import dotenv from "dotenv";
import cors, { CorsOptions } from "cors";
import mongoose from "mongoose";
import logger from "./utils/logger";
import { createServer } from "http";
import routes from "./routes";
dotenv.config();

import "./workers/watermark/batch.watermark.worker";

const app: Express = express();
const httpServer = createServer(app);

// MONGODB CONNECTION
const connect = async () => {
  const mongoUri = process.env.MONGO_URI!;
  try {
    await mongoose.connect(mongoUri);
    logger.info("Connected to MongoDB");
  } catch (err) {
    logger.error("MongoDB connection error: %O", err);
    throw err;
  }
};

// CORS POLICIES
const corsOptions: CorsOptions = {
  origin: [
    "https://www.lenslyst.com",
    "https://dashboard.lenslyst.com",
    "http://localhost:3000",
  ],
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  credentials: true,
  exposedHeaders: ["Set-Cookie", "Date", "ETag"],
};

// REQUEST LOGGING MIDDLEWARE
app.use((req, res, next) => {
  const start = Date.now();

  // Log the incoming request
  logger.info(`${req.method} ${req.originalUrl}`, {
    method: req.method,
    url: req.originalUrl,
    ip: req.ip,
    userAgent: req.get("User-Agent"),
    timestamp: new Date().toISOString(),
  });

  // Log the response when it finishes
  res.on("finish", () => {
    const duration = Date.now() - start;
    logger.info(`${req.method} ${req.originalUrl} - ${res.statusCode}`, {
      method: req.method,
      url: req.originalUrl,
      statusCode: res.statusCode,
      duration: `${duration}ms`,
      ip: req.ip,
    });
  });

  next();
});

//ROUTES
app.get("/", (req, res) => {
  res.send("Welcome to Lenslyst");
});

// Add this health endpoint
app.get("/health", (req, res) => {
  res.status(200).json({ status: "OK", timestamp: new Date().toISOString() });
});

app.use("/collections", routes);

const errorHandler: ErrorRequestHandler = (err, req, res, next) => {
  const errorStatus = err.status || 500;
  const errorMessage = err.message || "Something went wrong";

  logger.error("Error: %s\nStack: %s", errorMessage, err.stack);

  res.status(errorStatus).json({
    success: false,
    status: errorStatus,
    message: errorMessage,
    stack: err.stack,
  });
};

// Use the error handler
app.use(errorHandler);

//PORT FOR LISTENING TO APP
httpServer.listen(`${process.env.PORT}`, () => {
  connect();
  logger.info(`Server is running on port ${process.env.PORT}`);
});
