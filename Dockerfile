# Zero-dependency service — no `npm install` step needed.
FROM node:22-alpine
WORKDIR /app
COPY package.json scheduler.mjs ./
ENV NODE_ENV=production
# Persist the store on a volume (see docker-compose.yml).
ENV STORE_FILE=/data/store.json
EXPOSE 4500
CMD ["node", "scheduler.mjs"]
