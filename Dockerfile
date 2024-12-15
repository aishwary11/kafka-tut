FROM node:22-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install && npm cache clean --force
COPY . .
EXPOSE 3000
ENTRYPOINT ["node", "server.js"]