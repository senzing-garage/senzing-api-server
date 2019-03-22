ARG BASE_IMAGE=senzing/python-db2-base

# -----------------------------------------------------------------------------
# Stage: builder
# -----------------------------------------------------------------------------

FROM java:8 as builder

ENV REFRESHED_AT=2019-03-21

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
# Final stage
# -----------------------------------------------------------------------------

FROM ${BASE_IMAGE}

ENV REFRESHED_AT=2019-03-19

LABEL Name="senzing/senzing-api-server" \
      Version="1.0.0"

# Build arguments.

ARG SENZING_API_SERVER_JAR_VERSION=1.5.1

# Install packages via apt.

RUN apt-get update \
 && apt-get -y install \
      curl \
      default-jdk \
      gnupg \
      jq \
      lsb-core \
      lsb-release \
      odbc-postgresql \
      postgresql-client \
      python-dev \
      python-pip \
      python-pyodbc \
      sqlite \
      unixodbc \
      unixodbc-dev \
      wget \
 && rm -rf /var/lib/apt/lists/*

# Install libmysqlclient21.

ENV DEBIAN_FRONTEND=noninteractive
RUN wget -qO - https://repo.mysql.com/RPM-GPG-KEY-mysql | apt-key add - \
 && wget https://repo.mysql.com/mysql-apt-config_0.8.11-1_all.deb \
 && dpkg --install mysql-apt-config_0.8.11-1_all.deb \
 && apt-get update \
 && apt-get -y install libmysqlclient21 \
 && rm mysql-apt-config_0.8.11-1_all.deb \
 && rm -rf /var/lib/apt/lists/*

# Create MySQL connector.
# References:
#  - https://dev.mysql.com/downloads/connector/odbc/
#  - https://dev.mysql.com/doc/connector-odbc/en/connector-odbc-installation-binary-unix-tarball.html

RUN wget https://cdn.mysql.com//Downloads/Connector-ODBC/8.0/mysql-connector-odbc-8.0.13-linux-ubuntu18.04-x86-64bit.tar.gz \
 && tar -xvf mysql-connector-odbc-8.0.13-linux-ubuntu18.04-x86-64bit.tar.gz \
 && cp mysql-connector-odbc-8.0.13-linux-ubuntu18.04-x86-64bit/lib/* /usr/lib/x86_64-linux-gnu/odbc/ \
 && mysql-connector-odbc-8.0.13-linux-ubuntu18.04-x86-64bit/bin/myodbc-installer -d -a -n "MySQL" -t "DRIVER=/usr/lib/x86_64-linux-gnu/odbc/libmyodbc8w.so;" \
 && rm mysql-connector-odbc-8.0.13-linux-ubuntu18.04-x86-64bit.tar.gz \
 && rm -rf mysql-connector-odbc-8.0.13-linux-ubuntu18.04-x86-64bit

# Service exposed on port 8080

EXPOSE 8080

# Copy files from builder step.

COPY --from=builder "/git-repository/target/senzing-api-server-${SENZING_API_SERVER_JAR_VERSION}.jar" "/app/senzing-api-server.jar" 

# Set environment variables.

ENV SENZING_ROOT=/opt/senzing
ENV PYTHONPATH=${SENZING_ROOT}/g2/python
ENV LD_LIBRARY_PATH=${SENZING_ROOT}/g2/lib:${SENZING_ROOT}/g2/lib/debian;

# Runtime execution.

WORKDIR /app

ENTRYPOINT ["/app/docker-entrypoint.sh", "java -jar senzing-api-server.jar" ]
CMD [""]
