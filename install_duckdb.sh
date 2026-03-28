# =========================
# BUILD STAGE
# =========================
FROM golang:1.26-bookworm AS build

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    libc6-dev \
    pkg-config \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Force CGO ON for DuckDB Go bindings
ENV CGO_ENABLED=1

RUN go build -o porter ./cmd/porter


# =========================
# RUNTIME STAGE
# =========================
FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libstdc++6 \
    libgcc-s1 \
    && rm -rf /var/lib/apt/lists/*


# =========================
# OPTIONAL: DEBUG TOOLS (remove in prod if desired)
# =========================
RUN apt-get update && apt-get install -y \
    bash \
    curl \
    file \
    && rm -rf /var/lib/apt/lists/*


# =========================
# SYSTEM CONFIG
# =========================
ENV GODEBUG=cgocheck=0

# IMPORTANT: no external DuckDB libs
# DuckDB is embedded via Go binding


# =========================
# COPY BINARY
# =========================
COPY --from=build /app/porter /usr/local/bin/porter


# =========================
# RUNTIME CONFIG
# =========================
EXPOSE 32010

ENTRYPOINT ["porter"]
