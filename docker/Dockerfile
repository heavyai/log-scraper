FROM ubuntu:18.04
RUN apt-get update && apt-get install -y curl build-essential
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o installrust.sh && sh installrust.sh -y
RUN mkdir /log-scraper-internal
WORKDIR /log-scraper-internal
CMD /root/.cargo/bin/cargo build --release && echo "check target/release/ for the omnisci-log-scraper binary"
