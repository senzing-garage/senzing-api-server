ARG BASE_IMAGE=senzing/senzing-base

# -----------------------------------------------------------------------------
# Stage: builder
# -----------------------------------------------------------------------------

FROM java:8 as builder

ENV REFRESHED_AT=2019-03-22

LABEL Name="senzing/senzing-api-server-builder" \
      Version="1.0.0"

# Build arguments.

ARG SENZING_G2_JAR_RELATIVE_PATHNAME=target/g2.jar
ARG SENZING_G2_JAR_VERSION=1.5.0
ARG SENZING_API_SERVER_JAR_VERSION=1.5.1

# Install packages via apt.

RUN apt-get update
RUN apt-get -y install \
      make \
      maven \
 && rm -rf /var/lib/apt/lists/*

# Copy the repository from the local host.

COPY . /git-repository

# Run the "make" command to create the artifacts.

WORKDIR /git-repository

ENV SENZING_G2_JAR_PATHNAME=/git-repository/${SENZING_G2_JAR_RELATIVE_PATHNAME}
ENV SENZING_G2_JAR_VERSION=${SENZING_G2_JAR_VERSION}
ENV SENZING_API_SERVER_JAR_VERSION=${SENZING_API_SERVER_JAR_VERSION}

RUN make package

# -----------------------------------------------------------------------------
# Stage: Final
# -----------------------------------------------------------------------------

FROM ${BASE_IMAGE}

ENV REFRESHED_AT=2019-03-22

LABEL Name="senzing/senzing-api-server" \
      Version="1.0.0"

# Build arguments.

ARG SENZING_API_SERVER_JAR_VERSION=1.5.1

# Install packages via apt.

RUN apt-get update \
 && apt-get -y install \
      default-jdk \
 && rm -rf /var/lib/apt/lists/*

# Service exposed on port 8080

EXPOSE 8080

# Copy files from builder step.

COPY --from=builder "/git-repository/target/senzing-api-server-${SENZING_API_SERVER_JAR_VERSION}.jar" "/app/senzing-api-server.jar"

# Runtime execution.

WORKDIR /app

ENTRYPOINT ["/app/docker-entrypoint.sh", "java -jar senzing-api-server.jar" ]
CMD [""]
