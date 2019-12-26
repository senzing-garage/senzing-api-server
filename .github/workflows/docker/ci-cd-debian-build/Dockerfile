FROM debian:buster

ARG GITHUB_HEAD_REF="master"
ARG GITHUB_OWNER="Senzing"
ARG GITHUB_EVENT_NAME="push"
ARG SENZING_REPO_ENV="PRD"

LABEL Name="senzing-ci-cd-debian-builder" \
      Maintainer="support@senzing.com" \
      Version="1.0.0"

# Install packages via apt.
RUN echo "deb http://ftp.us.debian.org/debian sid main" >> /etc/apt/sources.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        apt-transport-https \
        git \
        gnupg2 \
        jq \
        make \
        openjdk-8-jdk \
        sudo \
        wget

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        maven \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables.
ENV SENZING_ACCEPT_EULA=I_ACCEPT_THE_SENZING_EULA

# Add Senzing Files
RUN if [[ "${SENZING_REPO_ENV}" == "PRD" ]]; then \
        SENZING_REPO_URL="https://senzing-production-apt.s3.amazonaws.com/senzingrepo_1.0.0-1_amd64.deb"; \
    else \
        SENZING_REPO_URL="https://senzing-staging-apt.s3.amazonaws.com/senzingstagingrepo_1.0.0-1_amd64.deb"; \
    fi \
    && wget ${SENZING_REPO_URL} \
    && apt-get -y install ./senzingstagingrepo_1.0.0-1_amd64.deb \
    && apt-get -y update \
    && rm -rf senzingrepo_1.0.0-1_amd64.deb \
    && apt-get -y install \
        senzingapi

# Set environment variables.
ENV SENZING_ROOT=/opt/senzing
ENV SENZING_G2_DIR=${SENZING_ROOT}/g2
ENV PYTHONPATH=${SENZING_ROOT}/g2/python
ENV LD_LIBRARY_PATH=${SENZING_ROOT}/g2/lib:${SENZING_ROOT}/g2/lib/debian

# Clone Senzing API Server repository.
WORKDIR /
RUN git clone https://github.com/${GITHUB_OWNER}/senzing-api-server.git

# Build Senzing API Server
WORKDIR /senzing-api-server
RUN git checkout ${GITHUB_HEAD_REF}; \
    if [[ "${GITHUB_HEAD_REF}" != "master" && ${GITHUB_EVENT_NAME} == "pull_request" ]]; then \
        git merge master; \
    fi

RUN export SENZING_API_SERVER_JAR_VERSION=$(mvn "help:evaluate" -Dexpression=project.version -q -DforceStdout) \
    && make \
        SENZING_G2_JAR_VERSION=$(cat ${SENZING_G2_DIR}/g2BuildVersion.json | jq --raw-output '.VERSION') \
        package
