# ---- build stage ----
FROM golang:1.26.1-bookworm AS build

WORKDIR /app

# Install full CGO toolchain
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    libc6-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# IMPORTANT: let Go detect arch automatically (do NOT force GOARCH)
RUN CGO_ENABLED=1 go build -o porter ./cmd/porter


# ---- runtime stage ----
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=build /app/porter /usr/local/bin/porter

EXPOSE 32010

ENTRYPOINT ["porter"]
