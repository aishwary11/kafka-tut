FROM node:22-alpine
WORKDIR /usr/src/app
COPY package*.json ./
RUN npm ci -f
COPY . .
EXPOSE 3000
CMD ["node", "server.js"]