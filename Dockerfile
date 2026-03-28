# ---- build stage ----
FROM golang:1.26.1-bookworm AS build

WORKDIR /app

# Install full CGO toolchain and tools for packaging DuckDB runtime
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    libc6-dev \
    pkg-config \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build the Porter binary with CGO enabled
RUN CGO_ENABLED=1 go build -o porter ./cmd/porter

# Download the DuckDB shared library that matches this build architecture and install it to /usr/local/lib
RUN arch=$(uname -m) \
    && case "$arch" in \
        x86_64) duckdb_arch=amd64 ;; \
        aarch64|arm64) duckdb_arch=arm64 ;; \
        *) echo "unsupported architecture: $arch" >&2; exit 1 ;; \
    esac \
    && curl -L -o /tmp/libduckdb-linux-${duckdb_arch}.zip \
        https://github.com/duckdb/duckdb/releases/download/v1.5.1/libduckdb-linux-${duckdb_arch}.zip \
    && unzip /tmp/libduckdb-linux-${duckdb_arch}.zip -d /usr/local/lib \
    && printf '%s\n' 'extern "C" int duckdb_adbc_init(int version, void *driver, void *error);' \
        'extern "C" int AdbcDriverInit(int version, void *driver, void *error) {' \
        '    return duckdb_adbc_init(version, driver, error);' \
        '}' \
        'extern "C" int AdbcDuckdbInit(int version, void *driver, void *error) {' \
        '    return duckdb_adbc_init(version, driver, error);' \
        '}' > /tmp/adbc_wrapper.cpp \
    && g++ -shared -fPIC -o /usr/local/lib/duckdb /tmp/adbc_wrapper.cpp -L/usr/local/lib -lduckdb -Wl,-rpath=/usr/local/lib \
    && chmod +x /usr/local/lib/duckdb \
    && rm /tmp/libduckdb-linux-${duckdb_arch}.zip /tmp/adbc_wrapper.cpp

# ---- runtime stage ----
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libstdc++6 \
    libgcc-s1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=build /app/porter /usr/local/bin/porter
COPY --from=build /usr/local/lib/libduckdb.so /usr/local/lib/libduckdb.so
COPY --from=build /usr/local/lib/duckdb /usr/local/lib/duckdb

RUN ldconfig

ENV LD_LIBRARY_PATH=/usr/local/lib

EXPOSE 32010

ENTRYPOINT ["porter"]
