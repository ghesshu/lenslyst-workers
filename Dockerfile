# Use the official Bun image
FROM oven/bun:alpine

# Set the working directory
WORKDIR /app

# Copy package files first for better caching
COPY package.json bun.lockb* ./

# Install dependencies
RUN bun install --production && bun pm cache rm

# Copy the rest of the application
COPY . .

# Build TypeScript to JavaScript
RUN bun run build

# Create non-root user for security
RUN addgroup -g 1001 -S bunjs && \
    adduser -S bunuser -u 1001 && \
    chown -R bunuser:bunjs /app
USER bunuser

# Expose the port
EXPOSE 3000

# Health check for Coolify
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD bun -e "const res = await fetch('http://localhost:3000/health'); process.exit(res.status === 200 ? 0 : 1)" || exit 1

# Run the TypeScript app with bun
CMD ["bun", "run", "app.ts"]