FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod ./
RUN go mod download || true

COPY . .

RUN go build -o service .

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /app/service .

ENV PORT=8082
EXPOSE 8082

CMD ["/app/service"]
