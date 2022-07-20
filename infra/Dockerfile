# syntax=docker/dockerfile:1.0.0-experimental
#FROM nvidia/cuda:11.3.1-base-ubuntu20.04
#FROM python:3.9.13-slim-bullseye
FROM ubuntu:20.04

# Env
ENV PYTHON_VERSION 3.9.13
ENV PYTHONUNBUFFERED=1
ENV TZ=UTC
ENV DEBIAN_FRONTEND=noninteractive

# Install deps
RUN apt-get update && \
    apt-get install -y --no-install-recommends make build-essential \
        libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev \
        wget ca-certificates curl llvm libncurses5-dev xz-utils tk-dev \
        libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev mecab-ipadic-utf8 \
        openssh-client git software-properties-common

# Pyenv
ENV PYENV_ROOT /root/.pyenv
ENV PATH $PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH
RUN set -ex \
    && curl https://pyenv.run | bash \
    && pyenv update \
    && pyenv install $PYTHON_VERSION \
    && pyenv global $PYTHON_VERSION \
    && pyenv rehash

# Install GDAL
RUN add-apt-repository ppa:ubuntugis/ubuntugis-unstable && \
    apt-get update && \
    apt-get install -y gdal-bin libgdal-dev && \
    pip install --global-option=build_ext \
        --global-option="-I/usr/include/gdal" \
        GDAL==`gdal-config --version`

# Install kubectl from Docker Hub.
COPY --from=lachlanevenson/k8s-kubectl:v1.10.3 /usr/local/bin/kubectl /usr/local/bin/kubectl

# Install basic requirements
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Fix port-forward issue
# Can remove once this is released to PyPI
# https://github.com/dask/dask-kubernetes/commit/ba2f3aa6390f5731f01942e3bb7f501921ef67e9
RUN pip install git+https://github.com/dask/dask-kubernetes

# Install oxeo-flows
ARG WATER=torch
COPY . oxeo-flows
RUN mkdir -p -m 0600 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts
RUN --mount=type=ssh pip install ./oxeo-flows[$WATER]
