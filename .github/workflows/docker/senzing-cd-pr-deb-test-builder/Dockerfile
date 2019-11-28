FROM debian:buster

ARG BRANCH="master"
ARG GITHUB_OWNER="Senzing"

LABEL Name="senzing-ci-cd-deb-test-builder" \
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
RUN wget https://senzing-production-apt.s3.amazonaws.com/senzingrepo_1.0.0-1_amd64.deb \
   && apt-get -y install ./senzingrepo_1.0.0-1_amd64.deb \
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

# Test merge into master locally
WORKDIR /senzing-api-server
RUN git checkout ${BRANCH} \
    && git merge master

# Build Senzing API Server
RUN export SENZING_API_SERVER_JAR_VERSION=$(mvn "help:evaluate" -Dexpression=project.version -q -DforceStdout) \
 && make \
      SENZING_G2_JAR_VERSION=$(cat ${SENZING_G2_DIR}/g2BuildVersion.json | jq --raw-output '.VERSION') \
      package
