# Use a lightweight Node image
FROM node:20-slim

# Create app directory
WORKDIR /usr/src/app

# Install dependencies
COPY package*.json ./
RUN npm install --only=production

# Copy app source
COPY . .

# Cloud Run will set PORT, but default to 8080 for local
ENV PORT=8080

# Start the app
CMD ["npm", "start"]
