FROM golang:1.21 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o kvstore .

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/kvstore .
COPY static/ ./static/
EXPOSE 8080
CMD ["./kvstore"]