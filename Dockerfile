FROM node:20-alpine
WORKDIR /usr/src/app
COPY package*.json ./
RUN yarn install -f
COPY . .
EXPOSE 3000
CMD ["node", "server.js"]