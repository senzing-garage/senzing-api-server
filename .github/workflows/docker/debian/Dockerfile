FROM python:3.8.0-buster

# Install packages via apt.
RUN apt-get update \
    && apt-get -y install \
        apt-transport-https \
        gnupg2 \
        wget \
        sudo \
        make \
        openjdk-11-jdk \
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

# Build Senzing API Server
RUN mkdir -p /app \
   && git clone https://github.com/Senzing/senzing-api-server.git \
   && cd /senzing-api-server \
   && export SENZING_API_SERVER_JAR_VERSION=$(mvn "help:evaluate" -Dexpression=project.version -q -DforceStdout) \
   && make \
        SENZING_G2_JAR_VERSION=$(grep -oP "(?<=(\"VERSION\"\:\s\"))(\d{1,}\.\d{1,}\.\d{1,})(?=\",)" ${SENZING_G2_DIR}/g2BuildVersion.json) \
        package
