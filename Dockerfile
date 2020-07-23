ARG BASE_IMAGE=senzing/senzing-base:1.5.2
ARG BASE_BUILDER_IMAGE=senzing/base-image-debian:1.0.3

# -----------------------------------------------------------------------------
# Stage: builder
# -----------------------------------------------------------------------------

FROM ${BASE_BUILDER_IMAGE} as builder

# Set Shell to use for RUN commands in builder step.

ENV REFRESHED_AT=2020-04-24

LABEL Name="senzing/senzing-api-server-builder" \
      Maintainer="support@senzing.com" \
      Version="1.0.0"

# Build arguments.

ARG SENZING_G2_JAR_RELATIVE_PATHNAME=unknown
ARG SENZING_G2_JAR_VERSION=unknown

# Set environment variables.

ENV SENZING_ROOT=/opt/senzing
ENV SENZING_G2_DIR=${SENZING_ROOT}/g2
ENV PYTHONPATH=${SENZING_ROOT}/g2/python
ENV LD_LIBRARY_PATH=${SENZING_ROOT}/g2/lib:${SENZING_ROOT}/g2/lib/debian

# Copy Repo files to Builder step.

COPY . /senzing-api-server

# Run the "make" command to create the artifacts.

WORKDIR /senzing-api-server

RUN export SENZING_API_SERVER_JAR_VERSION=$(mvn "help:evaluate" -Dexpression=project.version -q -DforceStdout) \
 && make \
     SENZING_G2_JAR_PATHNAME=/senzing-api-server/${SENZING_G2_JAR_RELATIVE_PATHNAME} \
     SENZING_G2_JAR_VERSION=${SENZING_G2_JAR_VERSION} \
     package \
 && cp /senzing-api-server/target/senzing-api-server-${SENZING_API_SERVER_JAR_VERSION}.jar "/senzing-api-server.jar"

# -----------------------------------------------------------------------------
# Stage: Final
# -----------------------------------------------------------------------------

FROM ${BASE_IMAGE}

ENV REFRESHED_AT=2020-04-24

LABEL Name="senzing/senzing-api-server" \
      Maintainer="support@senzing.com" \
      Version="1.8.3"

HEALTHCHECK CMD ["/app/healthcheck.sh"]

# Run as "root" for system installation.

USER root

# Install packages via apt.

RUN apt update \
 && apt -y install \
      software-properties-common \
 && rm -rf /var/lib/apt/lists/*

# Install Java-11.

RUN wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add - \
 && add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ \
 && apt update \
 && apt install -y adoptopenjdk-11-hotspot \
 && rm -rf /var/lib/apt/lists/*

# Service exposed on port 8080.

EXPOSE 8080

# Copy files from builder step.

COPY --from=builder "/senzing-api-server.jar" "/app/senzing-api-server.jar"

# Make non-root container.

USER 1001

# Runtime execution.

WORKDIR /app
ENTRYPOINT ["java", "-jar", "senzing-api-server.jar"]
