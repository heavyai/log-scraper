# FROM ubuntu:18.04
# FROM omnisci/core-os-cpu:v5.3.1
FROM docker-internal.mapd.com/omnisci/omnisci-ee-cpu:5.3.2rc3


RUN apt-get update && apt-get install -y curl build-essential
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o installrust.sh && sh installrust.sh -y

RUN apt-get update && apt-get install -y \
        python3.7 \
        python3-pip
RUN pip3 install \
        "pymapd==0.24.1" \
        "pyarrow==0.15.1"

RUN mkdir /src
# WORKDIR /src
# CMD cd /src && /root/.cargo/bin/cargo build --release && echo "check target/release/ for the omnisci-log-scraper binary"
