FROM rust as builder

RUN apt-get update && apt-get install -y \
	libssl-dev \
	pkg-config \
	clang \
	llvm

WORKDIR /server

# Copy the Cargo files to cache dependencies
COPY Cargo.toml Cargo.lock ./

# create a dummy file to cache dependencies
RUN mkdir src/ \
    && echo "fn main() {println!(\"If you see this, the build broke\")}" > src/main.rs \
    && cargo build --release \
    && rm -rf src/

COPY src/ ./src/

RUN cargo build --release


# execution environment
FROM debian:bookworm-slim

ENV RUST_LOG=info

RUN apt-get update && apt-get install -y openssl ca-certificates

COPY --from=builder /server/target/release/cache_feeder /usr/local/bin/server

ENTRYPOINT ["server"]
CMD ["--help"]

EXPOSE 3000