FROM golang:1.22-alpine AS build_base

RUN apk add --no-cache git

# Set the Current Working Directory inside the container
WORKDIR /tmp/snowflake-exporter

# We want to populate the module cache based on the go.{mod,sum} files.
COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

# Unit tests
RUN CGO_ENABLED=0 go test -v

# Build the Go app
RUN go build -o ./out/snowflake-exporter .

# Start fresh from a smaller image
FROM alpine:3.19
RUN apk add ca-certificates

COPY --from=build_base /tmp/snowflake-exporter/out/snowflake-exporter /app/snowflake-exporter

# This container exposes port 8080 to the outside world
EXPOSE 8080

# Run the binary program produced by `go install`
CMD ["/app/snowflake-exporter"]