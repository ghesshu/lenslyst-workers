# Use Node.js 18 LTS as base image (better compatibility with your dependencies)
FROM node:18-alpine

# Set working directory
WORKDIR /app

# Install system dependencies needed for Sharp, Canvas, and native modules
RUN apk add --no-cache \
    vips-dev \
    libc6-compat \
    python3 \
    make \
    g++ \
    cairo-dev \
    jpeg-dev \
    pango-dev \
    giflib-dev \
    pixman-dev \
    pangomm-dev \
    libjpeg-turbo-dev \
    freetype-dev \
    fontconfig \
    ttf-dejavu \
    ttf-liberation \
    ttf-opensans


# Copy package files first for better Docker layer caching
COPY package*.json ./

# Install ALL dependencies (including dev dependencies for TypeScript compilation)
RUN npm ci

# Copy the rest of the application
COPY . .

# Build the TypeScript project
RUN npm run build

# Remove dev dependencies and source files to reduce image size
RUN npm prune --production && \
    rm -rf src/ tsconfig.json node_modules/.cache
# Create logs directory
RUN mkdir -p logs

# Expose the port
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD node -e "require('http').get('http://localhost:3000/health', (res) => { process.exit(res.statusCode === 200 ? 0 : 1) })"

# Start the application
CMD ["npm", "start"]