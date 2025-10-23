ARG BASE_IMAGE=senzing/senzingapi-runtime:3.13.0@sha256:edca155d3601238fab622a7dd86471046832328d21f71f7bb2ae5463157f6e10
ARG BASE_BUILDER_IMAGE=senzing/base-image-debian:1.0.24@sha256:1e00881b45a78d9d93973ba845cd83d35aeb318273e30dde89060e01a9d0167c

# -----------------------------------------------------------------------------
# Stage: builder
# -----------------------------------------------------------------------------

FROM ${BASE_BUILDER_IMAGE} AS builder

ENV REFRESHED_AT=2024-06-24

# Run as "root" for system installation.

USER root

# Set environment variables.

ENV SENZING_ROOT=/opt/senzing
ENV SENZING_G2_DIR=${SENZING_ROOT}/g2
ENV PYTHONPATH=${SENZING_ROOT}/g2/sdk/python
ENV LD_LIBRARY_PATH=${SENZING_ROOT}/g2/lib:${SENZING_ROOT}/g2/lib/debian

# Build "senzing-api-server.jar".

COPY . /senzing-api-server
WORKDIR /senzing-api-server

RUN export SENZING_API_SERVER_VERSION=$(mvn "help:evaluate" -Dexpression=project.version -q -DforceStdout) \
  && make package \
  && cp /senzing-api-server/target/senzing-api-server-${SENZING_API_SERVER_VERSION}.jar "/senzing-api-server.jar"


# -----------------------------------------------------------------------------
# Stage: Final
# -----------------------------------------------------------------------------

FROM ${BASE_IMAGE}

ENV REFRESHED_AT=2024-06-24

LABEL Name="senzing/senzing-api-server" \
  Maintainer="support@senzing.com" \
  Version="3.5.20"

HEALTHCHECK CMD ["/app/healthcheck.sh"]

# Run as "root" for system installation.

USER root

# Install packages via apt-get.

RUN apt-get update \
  && apt-get -y --no-install-recommends install \
  gnupg2 \
  jq \
  libodbc2 \
  libodbccr2 \
  postgresql-client \
  unixodbc \
  && rm -rf /var/lib/apt/lists/*

# Install Java-11.

RUN mkdir -p /etc/apt/keyrings \
  && wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public > /etc/apt/keyrings/adoptium.asc

RUN echo "deb [signed-by=/etc/apt/keyrings/adoptium.asc] https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" >> /etc/apt/sources.list

RUN apt-get update \
  && apt-get install -y --no-install-recommends temurin-11-jdk \
  && rm -rf /var/lib/apt/lists/*

# Copy files from repository.

COPY ./rootfs /

# Set environment variables for root.

ENV LD_LIBRARY_PATH=/opt/senzing/g2/lib:/opt/senzing/g2/lib/debian:/opt/IBM/db2/clidriver/lib
ENV ODBCSYSINI=/etc/opt/senzing
ENV PATH=${PATH}:/opt/senzing/g2/python:/opt/IBM/db2/clidriver/adm:/opt/IBM/db2/clidriver/bin

# Service exposed on port 8080.

EXPOSE 8080

# Copy files from builder step.

COPY --from=builder "/senzing-api-server.jar" "/app/senzing-api-server.jar"

# Copy files from other docker containers.

COPY --from=senzing/senzing-api-server:3.5.20@sha256:4d92a0940ca6443a8c3b904a2a8b87d96d291c0745535c042c2e8ea3a113ae09 "/app/senzing-api-server.jar" "/appV2/senzing-api-server.jar"

# Make non-root container.

USER 1001

# Set environment variables for USER 1001.

ENV LD_LIBRARY_PATH=/opt/senzing/g2/lib:/opt/senzing/g2/lib/debian:/opt/IBM/db2/clidriver/lib
ENV ODBCSYSINI=/etc/opt/senzing
ENV PATH=${PATH}:/opt/senzing/g2/python:/opt/IBM/db2/clidriver/adm:/opt/IBM/db2/clidriver/bin
ENV SENZING_API_SERVER_BIND_ADDR=all

# Runtime execution.

WORKDIR /app

ENTRYPOINT ["/app/docker-entrypoint.sh"]
