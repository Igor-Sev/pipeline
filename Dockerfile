# Stage 1: Build
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o pipeline main.go

# Stage 2: Final (minimal size)
FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/pipeline .
CMD ["./pipeline"]