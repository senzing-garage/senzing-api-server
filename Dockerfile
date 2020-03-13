# -----------------------------------------------------------------------------
# Stage: builder
# -----------------------------------------------------------------------------
FROM debian:buster as builder

# Set Shell to use for RUN commands in builder step

ENV REFRESHED_AT=2019-11-13

LABEL Name="senzing/senzing-api-server-builder" \
      Maintainer="support@senzing.com" \
      Version="1.0.0"

# Install packages via apt.

RUN echo "deb http://ftp.us.debian.org/debian sid main" >> /etc/apt/sources.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        apt-transport-https \
        software-properties-common \
        git \
        openjdk-8-jdk \
        gnupg2 \
        jq \
        make \
        sudo \
        wget

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      maven \
 && rm -rf /var/lib/apt/lists/*

# Set environment variables.

ENV SENZING_ROOT=/opt/senzing
ENV SENZING_G2_DIR=${SENZING_ROOT}/g2
ENV PYTHONPATH=${SENZING_ROOT}/g2/python
ENV LD_LIBRARY_PATH=${SENZING_ROOT}/g2/lib:${SENZING_ROOT}/g2/lib/debian

# Set environment variables.

ENV SENZING_ACCEPT_EULA=I_ACCEPT_THE_SENZING_EULA

# Add Senzing Files

RUN wget https://senzing-production-apt.s3.amazonaws.com/senzingrepo_1.0.0-1_amd64.deb \
    && apt-get -y install ./senzingrepo_1.0.0-1_amd64.deb \
    && apt-get -y update \
    && rm -rf senzingrepo_1.0.0-1_amd64.deb \
    && apt-get -y install \
        senzingapi

# Clone Senzing API Server repository.

WORKDIR /
RUN git clone https://github.com/Senzing/senzing-api-server.git

WORKDIR /senzing-api-server

RUN export SENZING_API_SERVER_JAR_VERSION=$(mvn "help:evaluate" -Dexpression=project.version -q -DforceStdout) \
 && make \
     SENZING_G2_JAR_VERSION=$(cat ${SENZING_G2_DIR}/g2BuildVersion.json | jq --raw-output '.VERSION') \
     package \
 && mkdir -p /app \
 && cp /senzing-api-server/target/senzing-api-server-${SENZING_API_SERVER_JAR_VERSION}.jar "/app/senzing-api-server.jar"

# Create project
RUN python3 /opt/senzing/g2/python/G2CreateProject.py /app/senzing
RUN /bin/bash -c "source /app/senzing/setupEnv;  echo y | python3 /app/senzing/python/G2SetupConfig.py"
RUN python3 /app/senzing/python/G2Loader.py -P
RUN chown -R 1001 /app

# Service exposed on port 8080.

EXPOSE 8080

# Make non-root container.

USER 1001

# Runtime execution.

WORKDIR /app
ENTRYPOINT ["java", "-jar", "senzing-api-server.jar"]
