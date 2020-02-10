# -----------------------------------------------------------------------------
# Stage: builder
# -----------------------------------------------------------------------------
ARG BASE_BUILDER_IMAGE=senzing/base-image-debian:1.0.1
FROM ${BASE_BUILDER_IMAGE} as builder

# Set Shell to use for RUN commands in builder step

SHELL ["/bin/bash", "-c"]

ENV REFRESHED_AT=2019-11-13

LABEL Name="senzing/senzing-api-server-builder" \
      Maintainer="support@senzing.com" \
      Version="1.0.0"

# Build arguments.

ARG SENZING_G2_JAR_RELATIVE_PATHNAME=unknown
ARG SENZING_G2_JAR_VERSION=unknown
ARG GITHUB_HEAD_REF="master"
ARG GITHUB_OWNER="Senzing"
ARG GITHUB_EVENT_NAME="push"
ARG SENZING_G2_FILES="/opt/senzing"

# Set environment variables.

ENV SENZING_ROOT=/opt/senzing
ENV SENZING_G2_DIR=${SENZING_ROOT}/g2
ENV PYTHONPATH=${SENZING_ROOT}/g2/python
ENV LD_LIBRARY_PATH=${SENZING_ROOT}/g2/lib:${SENZING_ROOT}/g2/lib/debian

# Copy Senzing RPM Support Builder step.

COPY ${SENZING_G2_FILES} /opt/senzing

# Clone Senzing API Server repository.

WORKDIR /
RUN git clone https://github.com/${GITHUB_OWNER}/senzing-api-server.git

# Check If build is trigger by a git event, then chek.

WORKDIR /senzing-api-server
RUN git checkout ${GITHUB_HEAD_REF}; \
    [[ "${GITHUB_HEAD_REF}" != "master" && "${GITHUB_EVENT_NAME}" == "pull_request" ]] \
    && git merge master

# Run the "make" command to create the artifacts.

RUN export SENZING_API_SERVER_JAR_VERSION=$(mvn "help:evaluate" -Dexpression=project.version -q -DforceStdout) \
    && make \
        SENZING_G2_JAR_PATHNAME=/senzing-api-server/${SENZING_G2_JAR_RELATIVE_PATHNAME} \
        SENZING_G2_JAR_VERSION=$(cat ${SENZING_G2_DIR}/g2BuildVersion.json | jq --raw-output '.VERSION') \
        package \
    && cp /senzing-api-server/target/senzing-api-server-${SENZING_API_SERVER_JAR_VERSION}.jar "/senzing-api-server.jar"

# -----------------------------------------------------------------------------
# Stage: Final
# -----------------------------------------------------------------------------
ARG BASE_IMAGE=senzing/senzing-base:1.4.0
FROM ${BASE_IMAGE}

ENV REFRESHED_AT=2020-01-29

LABEL Name="senzing/senzing-api-server" \
      Maintainer="support@senzing.com" \
      Version="1.7.10"

HEALTHCHECK CMD ["/app/healthcheck.sh"]

# Run as "root" for system installation.

USER root

# Install packages via apt.

RUN apt update \
 && apt -y install \
      software-properties-common \
 && rm -rf /var/lib/apt/lists/*

# Install Java-8 - To be removed after Senzing API server supports Java 11
# Once fixed, add "default-jdk" to "apt install ..."

RUN wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add - \
 && add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ \
 && apt update \
 && apt -y install adoptopenjdk-8-hotspot

# Service exposed on port 8080.

EXPOSE 8080

# Copy files from builder step.

COPY --from=builder "/senzing-api-server.jar" "/app/senzing-api-server.jar"

# Make non-root container.

USER 1001

# Runtime execution.

WORKDIR /app
ENTRYPOINT ["java", "-jar", "senzing-api-server.jar"]
