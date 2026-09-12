# easyup has no runtime dependencies, so there is nothing to install and
# nothing to build - the image is just Node plus the source.
FROM node:22-alpine

ENV NODE_ENV=production \
    PORT=8080 \
    DATA_DIR=/data

WORKDIR /app

# Source only; see .dockerignore for what stays out.
COPY package.json ./
COPY src ./src
COPY public ./public
COPY scripts ./scripts

# Uploads land in a volume owned by the unprivileged `node` user that ships
# with the base image.
RUN mkdir -p /data && chown -R node:node /data /app

USER node
VOLUME ["/data"]
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:'+(process.env.PORT||8080)+'/api/health').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"

CMD ["node", "src/server.js"]
