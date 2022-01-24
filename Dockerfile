# ARG BASE_IMAGE=senzing/senzing-base:1.6.4
ARG BASE_IMAGE=debian:11.2@sha256:2906804d2a64e8a13a434a1a127fe3f6a28bf7cf3696be4223b06276f32f1f2d
ARG BASE_BUILDER_IMAGE=senzing/base-image-debian:1.0.7

# -----------------------------------------------------------------------------
# Stage: builder
# -----------------------------------------------------------------------------

FROM ${BASE_BUILDER_IMAGE} as builder

ENV REFRESHED_AT=2022-01-06

LABEL Name="senzing/senzing-api-server-builder" \
      Maintainer="support@senzing.com" \
      Version="1.1.1"

# Set environment variables.

ENV SENZING_ROOT=/opt/senzing
ENV SENZING_G2_DIR=${SENZING_ROOT}/g2
ENV PYTHONPATH=${SENZING_ROOT}/g2/python
ENV LD_LIBRARY_PATH=${SENZING_ROOT}/g2/lib:${SENZING_ROOT}/g2/lib/debian

# Build "senzing-api-server.jar".

COPY . /senzing-api-server
WORKDIR /senzing-api-server

RUN export SENZING_API_SERVER_VERSION=$(mvn "help:evaluate" -Dexpression=project.version -q -DforceStdout) \
      && make package \
      && cp /senzing-api-server/target/senzing-api-server-${SENZING_API_SERVER_VERSION}.jar "/senzing-api-server.jar"

# Grab a gpg key for our final stage to install the JDK

RUN wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public > /gpg.key

# -----------------------------------------------------------------------------
# Stage: Final
# -----------------------------------------------------------------------------

FROM ${BASE_IMAGE}

ENV REFRESHED_AT=2022-01-06

LABEL Name="senzing/senzing-api-server" \
      Maintainer="support@senzing.com" \
      Version="2.8.1"

HEALTHCHECK CMD ["/app/healthcheck.sh"]

# Run as "root" for system installation.

USER root

# Install packages via apt.

RUN apt update \
      && apt -y install \
      gnupg2 \
      software-properties-common \
      && rm -rf /var/lib/apt/lists/*

# Install Java-11.
COPY --from=builder "/gpg.key" "gpg.key"

RUN cat gpg.key | apt-key add - \
      && add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ \
      && apt update \
      && apt install -y adoptopenjdk-11-hotspot \
      && rm -rf /var/lib/apt/lists/* \
      && rm -f gpg.key

# Copy files from repository.

COPY ./rootfs /

# Service exposed on port 8080.

EXPOSE 8080

# Copy files from builder step.

COPY --from=builder "/senzing-api-server.jar" "/app/senzing-api-server.jar"

# Make non-root container.

USER 1001

# Runtime execution.

WORKDIR /app
ENTRYPOINT ["java", "-jar", "senzing-api-server.jar"]
