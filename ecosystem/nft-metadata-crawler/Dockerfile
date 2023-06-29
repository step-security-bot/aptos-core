# Start from the official Rust 1.70.0 image
FROM rust:1.70.0

# Install Postgres
RUN apt-get update && apt-get install -y postgresql

# Initialize Postgres
USER postgres
RUN service postgresql start &&\
    createuser --superuser docker &&\
    createdb -O docker docker &&\
    psql -c "ALTER USER docker PASSWORD 'docker';" &&\
    service postgresql stop

USER root
ENV DATABASE_URL=postgres://docker:docker@localhost:5432/postgres

# Copy your application code to the container
WORKDIR /nft-metadata-crawler
COPY . .
COPY Cargo.docker.toml Cargo.toml

# Build the application
RUN cargo install --path /nft-metadata-crawler
RUN cargo install diesel_cli --no-default-features --features postgres

# The command to run when the container starts
CMD service postgresql start && bash -c "diesel setup" && cargo run --bin nft-metadata-crawler
