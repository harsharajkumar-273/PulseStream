# --- Stage 1: Build Stage ---
FROM node:20-alpine AS builder

WORKDIR /usr/src/app

# Copy package descriptors
COPY package*.json ./
COPY tsconfig.json ./

# Install ALL dependencies (including devDependencies for compilation)
RUN npm ci

# Copy source code
COPY src ./src

# Compile TypeScript to JavaScript
RUN npm run build

# --- Stage 2: Production Runtime Stage ---
FROM node:20-alpine AS runner

WORKDIR /usr/src/app

# Set Node environment to production
ENV NODE_ENV=production

# Copy package files
COPY package*.json ./

# Install ONLY production dependencies to keep the image slim
RUN npm ci --only=production

# Copy compiled files from build stage
COPY --from=builder /usr/src/app/dist ./dist

# Run as non-privileged node user for security
USER node

# Expose ports (Gateway uses 3000, Consumer uses 3001)
EXPOSE 3000 3001

# The start command is overridden by docker-compose for each service
CMD ["node", "dist/index.js"]
