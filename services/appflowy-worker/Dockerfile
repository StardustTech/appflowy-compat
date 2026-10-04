# syntax=docker/dockerfile:1
ARG CHEF_IMAGE=lukemathwalker/cargo-chef:latest-rust-1.86
ARG RUNTIME_IMAGE=debian:bookworm-slim
FROM ${CHEF_IMAGE} as chef

# Set the initial working directory
WORKDIR /app
RUN sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list.d/debian.sources \
  && apt-get update && apt-get install -y lld clang

FROM chef as planner
COPY . .

# Compute a lock-like file for our project
RUN cargo chef prepare --recipe-path recipe.json

FROM chef as builder

# Update package lists and install protobuf-compiler along with other build dependencies
RUN apt update && apt install -y protobuf-compiler lld clang

# Keep dependency-stage arguments identical to the Cloud Dockerfile for cache reuse.
ARG FEATURES=""
ARG PROFILE="release"

COPY --from=planner /app/recipe.json recipe.json
# Build our project dependencies with optimized settings
ARG BUILD_JOBS=2
ENV CARGO_BUILD_JOBS=${BUILD_JOBS}
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
RUN echo "Building appflowy cloud with profile: ${PROFILE}"
RUN if [ "$PROFILE" = "debug" ]; then \
      cargo chef cook --locked --recipe-path recipe.json; \
    else \
      cargo chef cook --locked --profile "$PROFILE" --recipe-path recipe.json; \
    fi

COPY . .
ENV SQLX_OFFLINE=true

WORKDIR /app/services/appflowy-worker
RUN if [ "$PROFILE" = "debug" ]; then \
      cargo build --locked --bin appflowy_worker; \
    else \
      cargo build --locked --profile "$PROFILE" --bin appflowy_worker; \
    fi

FROM ${RUNTIME_IMAGE} AS runtime
WORKDIR /app/services/appflowy-worker
# Bootstrap HTTPS package verification from the pinned build image.
COPY --from=chef /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
RUN sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list.d/debian.sources \
  && apt-get update -y \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends openssl \
    && apt-get install -y -y ca-certificates \
    # Clean up
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app/
# Copy the binary from the appropriate target directory
ARG PROFILE="release"
RUN echo "Building worker with profile: ${PROFILE} "
RUN if [ "$PROFILE" = "release" ]; then \
      echo "Using release worker binary"; \
    else \
      echo "Using debug worker binary"; \
    fi
COPY --from=builder /app/target/$PROFILE/appflowy_worker /usr/local/bin/appflowy_worker
ENV APP_ENVIRONMENT production
ENV RUST_BACKTRACE  1
CMD ["appflowy_worker"]
