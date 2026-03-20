FROM ubuntu:22.04

RUN apt-get update && apt-get install -y git make wget build-essential curl

RUN wget --no-check-certificate https://go.dev/dl/go1.24.5.linux-amd64.tar.gz \
  && tar -xvf go1.24.5.linux-amd64.tar.gz -C /usr/local \
  && rm go1.24.5.linux-amd64.tar.gz

ENV GOROOT=/usr/local/go
ENV GOPATH=$HOME/go
ENV PATH=$GOPATH/bin:$GOROOT/bin:$PATH

RUN curl -LO https://github.com/astral-sh/uv/releases/download/0.8.3/uv-x86_64-unknown-linux-gnu.tar.gz \
    && tar -xvzf uv-x86_64-unknown-linux-gnu.tar.gz \
    && chmod +x ./uv-x86_64-unknown-linux-gnu/uv ./uv-x86_64-unknown-linux-gnu/uvx \
    && mkdir -p ~/.local/bin \
    && mv ./uv-x86_64-unknown-linux-gnu/uv ./uv-x86_64-unknown-linux-gnu/uvx ~/.local/bin/ \
    && rm -rf uv-x86_64-unknown-linux-gnu uv-x86_64-unknown-linux-gnu.tar.gz

ENV PATH=~/.local/bin:$PATH
