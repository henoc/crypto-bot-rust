FROM amazonlinux:2023

# https://github.com/rust-lang/docker-rust/blob/master/Dockerfile-debian.template

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    RUST_VERSION=1.75.0 \
    RUSTUP_VERSION=1.26.0 \
    RUST_ARCH=x86_64-unknown-linux-gnu

RUN yum install -y wget gcc openssl-devel; \
    url="https://static.rust-lang.org/rustup/archive/${RUSTUP_VERSION}/${RUST_ARCH}/rustup-init"; \
    wget "$url"; \
    chmod +x rustup-init; \
    ./rustup-init -y --no-modify-path --default-toolchain $RUST_VERSION; \