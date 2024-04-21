# First stage: Go build environment
# Use an official Go image which includes all necessary tools to build Go applications
FROM golang:1.21-alpine AS builder

# Install git, gcc, musl-dev, and ca-certificates for Go modules and CGO builds
# You may also need to install librdkafka-dev if your Go application uses the Confluent Kafka library
RUN apk update && \
    apk add --no-cache git gcc musl-dev ca-certificates librdkafka-dev build-base && \
    update-ca-certificates

WORKDIR /build

COPY go.mod go.sum ./

# Download dependencies
RUN go mod download && go mod verify

# Copy the rest of the application source code
COPY . .

# Build the Go application
# If CGO is not required, set CGO_ENABLED=0 to avoid linking issues
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -ldflags='-w -s' -a -tags musl -o binary_app .

# Second stage: Minimal runtime environment with glibc compatibility
FROM frolvlad/alpine-glibc:alpine-3.14

# Copy the binary from the builder stage
COPY --from=builder /build/binary_app /app/binary_app

COPY --from=builder /build/.env /app/.env

WORKDIR /app

# Set necessary environmet variables needed by the app
ENV SOME_ENVIRONMENT_VARIABLE=value

# Run the compiled binary
ENTRYPOINT ["./binary_app"]