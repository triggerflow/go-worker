FROM golang:1.14.3-buster as builder
WORKDIR /triggerflow
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o triggerflow-worker .

FROM debian:buster-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates
RUN update-ca-certificates
WORKDIR /triggerflow
COPY --from=builder /triggerflow/triggerflow-worker .
ENTRYPOINT ["./triggerflow-worker"]