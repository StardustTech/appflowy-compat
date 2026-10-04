# syntax=docker/dockerfile:1
# Using cargo-chef to manage Rust build cache effectively
ARG CHEF_IMAGE=lukemathwalker/cargo-chef:latest-rust-1.86
ARG RUNTIME_IMAGE=debian:bookworm-slim
FROM ${CHEF_IMAGE} as chef

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

# Specify a default value for FEATURES; it could be an empty string if no features are enabled by default
ARG FEATURES=""
ARG PROFILE="release"

COPY --from=planner /app/recipe.json recipe.json
ARG BUILD_JOBS=2
ENV CARGO_BUILD_JOBS=${BUILD_JOBS}
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
# Reduce memory usage during compilation
RUN echo "Building appflowy cloud with profile: ${PROFILE}"
RUN if [ "$PROFILE" = "debug" ]; then \
      cargo chef cook --locked --recipe-path recipe.json; \
    else \
      cargo chef cook --locked --profile "$PROFILE" --recipe-path recipe.json; \
    fi

COPY . .
ENV SQLX_OFFLINE true

# Build the project
RUN echo "Building with profile: ${PROFILE}, features: ${FEATURES}, "
RUN if [ "$PROFILE" = "debug" ]; then \
      cargo build --locked --features "${FEATURES}" --bin appflowy_cloud; \
    else \
      cargo build --locked --profile "$PROFILE" --features "${FEATURES}" --bin appflowy_cloud; \
    fi

FROM ${RUNTIME_IMAGE} AS runtime
WORKDIR /app
RUN sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list.d/debian.sources \
  && apt-get update -y \
  && apt-get install -y --no-install-recommends openssl ca-certificates curl \
  && update-ca-certificates \
  # Clean up
  && apt-get autoremove -y \
  && apt-get clean -y \
  && rm -rf /var/lib/apt/lists/*

# Copy the binary from the appropriate target directory
ARG PROFILE="release"
RUN echo "Building with profile: ${PROFILE}"
RUN if [ "$PROFILE" = "release" ]; then \
      echo "Using release binary"; \
    else \
      echo "Using debug binary"; \
    fi
COPY --from=builder /app/target/$PROFILE/appflowy_cloud /usr/local/bin/appflowy_cloud
ENV APP_ENVIRONMENT production
ENV RUST_BACKTRACE 1

ARG APPFLOWY_APPLICATION_PORT
ARG PORT
ENV PORT=${APPFLOWY_APPLICATION_PORT:-${PORT:-8000}}
EXPOSE $PORT

CMD ["appflowy_cloud"]
