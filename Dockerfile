# syntax=docker/dockerfile:1

# Build the application from source
FROM golang:1.23 AS build-stage

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
COPY pkg ./pkg

RUN CGO_ENABLED=0 GOOS=linux go build -o /minio_cleaner

# Run the tests in the container
FROM build-stage AS run-test-stage
RUN go test -v ./...

# Deploy the application binary into a lean image
FROM gcr.io/distroless/base-debian11 AS build-release-stage

LABEL authors="justinisrael@gmail.com"

WORKDIR /var/run/minio_cleaner/db

COPY --from=build-stage /minio_cleaner /minio_cleaner

USER nonroot:nonroot

ENTRYPOINT ["/minio_cleaner"]