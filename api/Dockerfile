# Build stage
FROM golang:1.25-alpine AS build
WORKDIR /src

# Copy go mod files and download dependencies
COPY api/go.mod api/go.sum ./api/
WORKDIR /src/api
RUN go mod download

# Copy source and build with optimizations
COPY api /src/api
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-w -s" \
    -o /out/api ./cmd/api

# Final stage - using distroless
FROM gcr.io/distroless/static-debian12
WORKDIR /app

COPY --from=build /out/api /app/api
COPY etl /app/etl

EXPOSE 8000
CMD ["/app/api"]

