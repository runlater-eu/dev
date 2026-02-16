FROM golang:1.23-alpine AS build
WORKDIR /app
COPY go.mod ./
COPY *.go ./
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o runlater-dev .

FROM alpine:3.21
RUN apk --no-cache add ca-certificates
COPY --from=build /app/runlater-dev /usr/local/bin/
EXPOSE 8080
ENTRYPOINT ["runlater-dev"]
