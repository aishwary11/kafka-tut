FROM node:20-alpine
WORKDIR /usr/src/app
COPY package*.json ./
RUN npm i -f
COPY . .
EXPOSE 3000
CMD ["node", "server.js"]