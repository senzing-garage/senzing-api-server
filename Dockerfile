
# ----- Stage #1 - builder ----------------------------------------------------

ARG BASE_CONTAINER=java:8
FROM ${BASE_CONTAINER} as builder

ARG REFRESHED_AT=2018-09-17
ARG GIT_REPOSITORY_NAME=unknown

RUN apt-get update
RUN apt-get -y install \
      make \
      maven \
 && rm -rf /var/lib/apt/lists/*
 
# Copy the repository from the local host.

COPY . /${GIT_REPOSITORY_NAME}

# Copy ...

ARG SENZING_G2_JAR_PATHNAME=/opt/senzing/g2/lib/g2.jar

RUN mkdir --parents $(dirname ${SENZING_G2_JAR_PATHNAME}) \
 && cp /${GIT_REPOSITORY_NAME}/target/g2.jar ${SENZING_G2_JAR_PATHNAME} 

# Run the "make" command to create the artifacts.

WORKDIR /${GIT_REPOSITORY_NAME}
RUN make package

# ----- Stage #2 --------------------------------------------------------------

ARG BASE_CONTAINER=java:latest
FROM ${BASE_CONTAINER}

# Service exposed on port 8080

EXPOSE 8080

# Copy files from host system.

RUN mkdir /app
COPY --from=builder /${GIT_REPOSITORY_NAME}/target/senzing-api-server-1.5.0.jar /app/senzing-api-server.jar

# Set environment variables.

ENV SENZING_DIR=/opt/senzing
ENV PYTHONPATH=${SENZING_DIR}/g2/python
ENV LD_LIBRARY_PATH=${SENZING_DIR}/g2/lib:${SENZING_DIR}/g2/lib/debian;
ENV SENZING_CONCURRENCY=8
ENV SENZING_BIND_ADDR=all
ENV SENZING_INI_FILE=${SENZING_DIR}/g2/python/G2Module.ini

# Run the Senzing REST api server.

WORKDIR /app

CMD java -jar senzing-api-server.jar \
     -concurrency ${SENZING_CONCURRENCY} \
     -httpPort 8080 \
     -bindAddr ${SENZING_BIND_ADDR} \
     -iniFile ${SENZING_INI_FILE}