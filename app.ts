import express, { Express, ErrorRequestHandler } from "express";
import dotenv from "dotenv";
import mongoose from "mongoose";
import logger from "./utils/logger";
import { createServer } from "http";
dotenv.config();

import "./workers/watermark/watermark.worker";

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

//ROUTES
app.get("/", (req, res) => {
  res.send("Welcome to Lenslyst");
});

// Add this health endpoint
app.get("/health", (req, res) => {
  res.status(200).json({ status: "OK", timestamp: new Date().toISOString() });
});

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
