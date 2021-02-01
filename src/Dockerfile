FROM node:10 as build

ADD . /app
WORKDIR /app
RUN npm ci --only=production

FROM gcr.io/distroless/nodejs:10
COPY --from=build /app /app
WORKDIR /app

CMD ["index.js"]