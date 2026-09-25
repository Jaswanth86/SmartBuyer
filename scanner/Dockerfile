FROM node:20-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY index.mjs ./
EXPOSE 8787
CMD ["npm","start"]