FROM centos:8

ARG GITHUB_HEAD_REF="master"
ARG GITHUB_OWNER="Senzing"
ARG GITHUB_EVENT_NAME="push"

LABEL Name="senzing-ci-cd-rhel-test-builder" \
      Maintainer="support@senzing.com" \
      Version="1.0.0"

# Install packages via apt.
RUN yum update -y \
    && yum install -y \
        git \
        jq \
        make \
        java-1.8.0-openjdk \
        sudo \
    && yum install -y \
        maven

# Set environment variables.
ENV SENZING_ACCEPT_EULA=I_ACCEPT_THE_SENZING_EULA

# Add Senzing Files
RUN yum install -y \
        https://senzing-production-yum.s3.amazonaws.com/senzingrepo-1.0.0-1.x86_64.rpm \
   && yum install -y \
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
