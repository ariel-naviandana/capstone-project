# k6-dashboard: Node.js server that runs k6 performance_test.js on demand
# and streams output to a web UI.

FROM node:20-alpine

# Install k6 from Alpine's community repo (avoids fetching binaries from GitHub,
# which can be blocked/unreachable in some Docker network configs)
RUN apk add --no-cache k6 ca-certificates

WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev

COPY server.js ./
COPY public ./public

ENV PORT=5555
EXPOSE 5555

CMD ["node", "server.js"]