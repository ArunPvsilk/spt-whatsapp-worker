FROM node:20-alpine

# Set working directory
WORKDIR /app

# Install dependencies needed for node native packages if any
RUN apk add --no-cache python3 make g++

# Copy package manifests
COPY package*.json ./

# Install dependencies
RUN npm install --omit=dev

# Copy application source
COPY . .

# Expose HTTP health port for Cloud Run
ENV PORT=8080
EXPOSE 8080

# Start worker
CMD ["node", "worker.js"]
