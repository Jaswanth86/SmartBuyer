FROM node:20-alpine
WORKDIR /app
COPY scanner/package.json ./package.json
RUN npm install --omit=dev
COPY scanner/index.mjs ./index.mjs
EXPOSE 8787
CMD ["npm","start"]
