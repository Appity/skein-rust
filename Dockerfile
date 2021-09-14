FROM rust:1.55-buster as builder

ARG GH_USER
ARG GH_TOKEN

RUN git config --global url."https://${GH_USER}:${GH_TOKEN}@github.com/".insteadOf "https://github.com/"

WORKDIR /src/skein-rust
COPY . .

RUN cargo install --path .

FROM debian:buster

RUN apt-get update && apt install libpq5 libssl1.1 -y

COPY --from=builder /usr/local/cargo/bin/amqp-client /usr/local/bin/amqp-client
COPY --from=builder /usr/local/cargo/bin/amqp-worker /usr/local/bin/amqp-worker

WORKDIR /usr/local/bin

CMD [ "/usr/local/bin/amqp-worker" ]
