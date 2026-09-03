# syntax=docker/dockerfile:1

FROM rust:1-bookworm

LABEL description="Lamellar runtime container image (default features)"

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y \
        build-essential clang libclang-dev cmake perl flex pkg-config ca-certificates \
        libhwloc-dev libibverbs-dev librdmacm-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /lamellar-runtime
COPY . .

RUN cargo fetch
RUN cargo build --release --example hello_world_am

RUN chmod -R a+rX /lamellar-runtime /usr/local/cargo

# Containers run as root by default; PRRTE (invoked internally by
# #[lamellar::main]) refuses to launch as root unless told otherwise.
ENV PRTE_ALLOW_RUN_AS_ROOT=1
ENV PRTE_ALLOW_RUN_AS_ROOT_CONFIRM=1

WORKDIR /lamellar-runtime
