# ----- Stage #1 - builder ----------------------------------------------------

ARG BASE_STAGE_1_CONTAINER=java:8
FROM ${BASE_STAGE_1_CONTAINER} as builder

ARG REFRESHED_AT=2018-09-17
ARG GIT_REPOSITORY_NAME=unknown
ARG SENZING_G2_JAR_PATHNAME=/opt/senzing/g2/lib/g2.jar
ARG SENZING_G2_JAR_VERSION=1.5.0

RUN apt-get update
RUN apt-get -y install \
      make \
      maven \
 && rm -rf /var/lib/apt/lists/*

# Copy the repository from the local host.

COPY . /${GIT_REPOSITORY_NAME}

# Copy g2.jar from repository directory to location expected by "make package".

RUN mkdir --parents $(dirname ${SENZING_G2_JAR_PATHNAME}) \
 && cp /${GIT_REPOSITORY_NAME}/target/g2.jar ${SENZING_G2_JAR_PATHNAME} 

# Run the "make" command to create the artifacts.

WORKDIR /${GIT_REPOSITORY_NAME}
RUN make package

# ----- Stage #2 --------------------------------------------------------------

ARG BASE_IMAGE=debian:9
FROM ${BASE_IMAGE}

ENV REFRESHED_AT=2019-03-14

LABEL Name="senzing/senzing-api-server" \
      Version="1.0.0"

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

# Copy files from host system.

RUN mkdir /app
COPY --from=builder /${GIT_REPOSITORY_NAME}/target/senzing-api-server-1.5.0.jar /app/senzing-api-server.jar

# Set environment variables.

ENV SENZING_ROOT=/opt/senzing
ENV PYTHONPATH=${SENZING_ROOT}/g2/python
ENV LD_LIBRARY_PATH=${SENZING_ROOT}/g2/lib:${SENZING_ROOT}/g2/lib/debian;

# App parameters.

ENV SENZING_BIND_ADDR=all
ENV SENZING_CONCURRENCY=8
ENV SENZING_INI_FILE=${SENZING_ROOT}/g2/python/G2Module.ini
ENV SENZING_OPTIONS=""

# Run the Senzing REST api server.

WORKDIR /app

CMD java -jar senzing-api-server.jar \
     -concurrency ${SENZING_CONCURRENCY} \
     -httpPort 8080 \
     -bindAddr ${SENZING_BIND_ADDR} \
     -iniFile ${SENZING_INI_FILE} \
     ${SENZING_OPTIONS}
