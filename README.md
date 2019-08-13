# senzing-api-server

## Overview

The Senzing Rest API Server implemented in Java.  The API specification is
defined in by the [Senzing Rest API Proposal](https://github.com/Senzing/senzing-rest-api).

The [Senzing API OAS specification](http://editor.swagger.io/?url=https://raw.githubusercontent.com/Senzing/senzing-rest-api/master/senzing-rest-api.yaml)
documents the available API methods, their parameters and the response formats.

### Contents

1. [Demonstrate using Command Line](#demonstrate-using-command-line)
    1. [Dependencies](#dependencies)
    1. [Building](#building)
    1. [Running](#running)
1. [Demonstrate using Docker](#demonstrate-using-docker)
    1. [Expectations for docker](#expectations-for-docker)
    1. [Get docker image](#get-docker-image)
    1. [Initialize Senzing](#initialize-senzing)
    1. [Configuration](#configuration)
    1. [Volumes](#volumes)
    1. [Run docker container](#run-docker-container)
    1. [Test docker container](#test-docker-container)
1. [Develop](#develop)
    1. [Prerequisite software](#prerequisite-software)
    1. [Clone repository](#clone-repository)
    1. [Build docker image for development](#build-docker-image-for-development)
1. [Examples](#examples)
1. [Errors](#errors)
1. [References](#references)

## Demonstrate using Command Line

### Dependencies

To build the Senzing REST API Server you will need Apache Maven (recommend version 3.5.4 or later)
as well as Java 1.8.x (recommend version 1.8.0_171 or later).

You will also need the Senzing "g2.jar" file installed in your Maven repository.
The Senzing REST API Server requires version 1.7.x or later of the Senzing API
and Senzing App.  In order to install g2.jar you must:

 1. Locate your [`${SENZING_G2_DIR}` directory](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/create-senzing-dir.md).
    The default locations are:
    - Linux Archive Extraction: `/opt/senzing/` (see [Install Instructions](https://github.com/Senzing/hello-senzing-springboot-java/blob/master/doc/debian-based-installation.md#install))
    - Windows MSI Installer: `C:\Program Files\Senzing\`

 1. Determine your `${SENZING_VERSION}` version number:
    - Locate your `g2BuildVersion.json` file:
        - Linux: `${SENZING_G2_DIR}/g2BuildVersion.json`
        - Windows: `${SENZING_G2_DIR}\g2BuildVersion.json`
    - Find the value for the `"VERSION"` property in the JSON contents.
      Example:

        ```console
        {
            "PLATFORM": "Linux",
            "VERSION": "1.7.19095",
            "BUILD_NUMBER": "2019_04_05__02_00"
        }
        ```

 1. Install the g2.jar file in your local Maven repository, replacing the
    `${SENZING_G2_DIR}` and `${SENZING_VERSION}` variables as determined above:

     - Linux:

       ```console
             export SENZING_G2_DIR=/opt/senzing/g2
             export SENZING_VERSION=1.7.19095

             mvn install:install-file \
                 -Dfile=${SENZING_G2_DIR}/lib/g2.jar \
                 -DgroupId=com.senzing \
                 -DartifactId=g2 \
                 -Dversion=${SENZING_VERSION} \
                 -Dpackaging=jar
       ```

     - Windows:

       ```console
             set SENZING_G2_DIR="C:\Program Files\Senzing\g2"
             set SENZING_VERSION=1.7.19095

             mvn install:install-file \
                 -Dfile=%SENZING_G2_DIR%\lib\g2.jar \
                 -DgroupId=com.senzing \
                 -DartifactId=g2 \
                 -Dversion=%SENZING_VERSION% \
                 -Dpackaging=jar
       ```

 1. Setup your environment.  The API's rely on native libraries and the
    environment must be properly setup to find those libraries:

    - Linux

       ```console
          export SENZING_G2_DIR=/opt/senzing/g2

          export LD_LIBRARY_PATH=${SENZING_G2_DIR}/lib:${SENZING_G2_DIR}/lib/debian:$LD_LIBRARY_PATH
       ```

    - Windows

      ```console
          set SENZING_G2_DIR="C:\Program Files\Senzing\g2"

          set Path=%SENZING_G2_DIR%\lib;$Path
      ```

### Building

To build simply execute:

   ```console
      mvn install
   ```

The JAR file will be contained in the `target` directory under the name `senzing-api-server-[version].jar`.

Where `[version]` is the version number from the `pom.xml` file.

### Running

To execute the server you will use `java -jar`.  It assumed that your environment
is properly configured as described in the "Dependencies" section above.

The only command-line option that is required is the `-iniFile` file option which
specifies the path to the INI file used to initialize the API.

***NOTE:*** *In lieu of using `java -jar` directly and the `-iniFile` option to
specify your entity repository, you can use the
[Senzing App Integration Scripts](./app-scripts/README.md) to start the
Senzing REST API Server using an entity repository from the
[Senzing app](https://senzing.com/#download).  The scripts are provided in the
`app-scripts` sub-directory.  See the [associated README.md file](./app-scripts/README.md)
for version compatibility and usage information.*

Other command-line options may be useful to you as well.  Execute
`java -jar target/senzing-api-server-1.6.0.jar -help` to obtain a help message
describing all available options.
Example:

  ```console
     cd target

    $ java -jar senzing-api-server-1.6.0.jar -help

    java -jar senzing-api-server-1.6.0.jar <options>

    <options> includes:
       -help
            Should be the first and only option if provided.
            Causes this help messasge to be displayed.
            NOTE: If this option is provided, the server will not start.

       -version
            Should be the first and only option if provided.
            Causes the version of the G2 REST API Server to be displayed.
            NOTE: If this option is provided, the server will not start.

       -httpPort [port-number]
            Sets the port for HTTP communication.  Defaults to 2080.
            Specify 0 for a randomly selected port number.

       -bindAddr [ip-address|loopback|all]
            Sets the port for HTTP bind address communication.
            Defaults to the loopback address.

       -allowedOrigins [url-domain]
            Sets the CORS Access-Control-Allow-Origin header for all endpoints.
            No Default.

       -concurrency [thread-count]
            Sets the number of threads available for executing
            Senzing API functions (i.e.: the number of engine threads).
            If not specified, then this defaults to 8.

       -moduleName [module-name]
            The module name to initialize with.  Defaults to 'ApiServer'.

       -iniFile [ini-file-path]
            The path to the Senzing INI file to with which to initialize.

       -verbose If specified then initialize in verbose mode.

       -monitorFile [filePath]
            Specifies a file whose timestamp is monitored to determine
            when to shutdown.
  ```

If you wanted to run the server on port 8080 and bind to all
network interfaces with a concurrency of 16 you would use:

  ```console
     java -jar target/senzing-api-server-[version].jar \
        -concurrency 16 \
        -httpPort 8080 \
        -bindAddr all \
        -iniFile /opt/senzing/g2/python/G2Module.ini
  ```

#### Restart for Configuration Changes

It is important to note that the Senzing configuration is currently read by the
Senzing API Server on startup.  If the configuration changes, the changes will
not be detected until the Server is restarted.  This may cause stale values to
be returned from some operations and may cause other operations to completely
fail.

Be sure to restart the API server when the configuration changes to guarantee
stability and accurate results from the API server.

## Demonstrate using Docker

### Expectations for docker

#### Space for docker

This repository and demonstration require 6 GB free disk space.

#### Time for docker

Budget 40 minutes to get the demonstration up-and-running, depending on CPU and network speeds.

#### Background knowledge for docker

This repository assumes a working knowledge of:

1. [Docker](https://github.com/Senzing/knowledge-base/blob/master/WHATIS/docker.md)

### Get docker image

1. Option #1. The `senzing/template` docker image is on [DockerHub](https://hub.docker.com/r/senzing/template) and can be downloaded.
   Example:

    ```console
    sudo docker pull senzing/template
    ```

1. Option #2. The `senzing/template` image can be built locally.
   See [Develop](#develop).

### Initialize Senzing

1. If Senzing has not been initialized, visit
   [HOWTO - Initialize Senzing](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/initialize-senzing.md).

### Configuration

Configuration values specified by environment variable or command line parameter.

- **[SENZING_DATA_VERSION_DIR](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_data_version_dir)**
- **[SENZING_DATABASE_URL](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_database_url)**
- **[SENZING_DEBUG](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_debug)**
- **[SENZING_ETC_DIR](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_etc_dir)**
- **[SENZING_G2_DIR](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_g2_dir)**
- **[SENZING_VAR_DIR](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_var_dir)**
- **[SENZING_WEBAPP_PORT](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_webapp_port)**

### Volumes

The output of `yum install senzingapi` placed files in different directories.
Create a folder for each output directory.

1. :pencil2: Option #1.
   To mimic an actual RPM installation,
   identify directories for RPM output in this manner:

    ```console
    export SENZING_DATA_VERSION_DIR=/opt/senzing/data/1.0.0
    export SENZING_ETC_DIR=/etc/opt/senzing
    export SENZING_G2_DIR=/opt/senzing/g2
    export SENZING_VAR_DIR=/var/opt/senzing
    ```

1. :pencil2: Option #2.
   If Senzing directories were put in alternative directories,
   set environment variables to reflect where the directories were placed.
   Example:

    ```console
    export SENZING_VOLUME=/opt/my-senzing

    export SENZING_DATA_VERSION_DIR=${SENZING_VOLUME}/data/1.0.0
    export SENZING_ETC_DIR=${SENZING_VOLUME}/etc
    export SENZING_G2_DIR=${SENZING_VOLUME}/g2
    export SENZING_VAR_DIR=${SENZING_VOLUME}/var
    ```

### Run docker container

#### Variation 1

Run docker container with internal SQLite database and external volume.

1. :pencil2: Set environment variables.
   Example:

    ```console
    export SENZING_WEBAPP_PORT=8889
    ```

1. Run the docker container.
   Example:

    ```console
    sudo docker run \
      --interactive \
      --publish ${SENZING_WEBAPP_PORT}:8080 \
      --rm \
      --tty \
      --volume ${SENZING_DATA_VERSION_DIR}:/opt/senzing/data \
      --volume ${SENZING_ETC_DIR}:/etc/opt/senzing \
      --volume ${SENZING_G2_DIR}:/opt/senzing/g2 \
      --volume ${SENZING_VAR_DIR}:/var/opt/senzing \
      senzing/senzing-api-server \
        -concurrency 10 \
        -httpPort 8080 \
        -bindAddr all \
        -iniFile /opt/senzing/g2/python/G2Module.ini
    ```

#### Variation 2

Run docker container accessing an external PostgreSQL database and volumes.

1. :pencil2: Set environment variables.
   Example:

    ```console
    export DATABASE_PROTOCOL=postgresql
    export DATABASE_USERNAME=postgres
    export DATABASE_PASSWORD=postgres
    export DATABASE_HOST=senzing-postgresql
    export DATABASE_PORT=5432
    export DATABASE_DATABASE=G2
    export SENZING_WEBAPP_PORT=8889
    ```

1. Run docker container.
   Example:

    ```console
    export SENZING_DATABASE_URL="${DATABASE_PROTOCOL}://${DATABASE_USERNAME}:${DATABASE_PASSWORD}@${DATABASE_HOST}:${DATABASE_PORT}/${DATABASE_DATABASE}"

    sudo docker run \
      --env SENZING_DATABASE_URL="${SENZING_DATABASE_URL}" \
      --interactive \
      --publish ${SENZING_WEBAPP_PORT}:8080 \
      --rm \
      --tty \
      --volume ${SENZING_DATA_VERSION_DIR}:/opt/senzing/data \
      --volume ${SENZING_ETC_DIR}:/etc/opt/senzing \
      --volume ${SENZING_G2_DIR}:/opt/senzing/g2 \
      --volume ${SENZING_VAR_DIR}:/var/opt/senzing \
      senzing/senzing-api-server \
        -concurrency 10 \
        -httpPort 8080 \
        -bindAddr all \
        -iniFile /opt/senzing/g2/python/G2Module.ini
    ```

#### Variation 3

Run docker container accessing an external MySQL database in a docker network.

1. :pencil2: Determine docker network.
   Example:

    ```console
    sudo docker network ls

    # Choose value from NAME column of docker network ls
    export SENZING_NETWORK=nameofthe_network
    ```

1. :pencil2: Set environment variables.
   Example:

    ```console
        export DATABASE_PROTOCOL=mysql
        export DATABASE_USERNAME=root
        export DATABASE_PASSWORD=root
        export DATABASE_HOST=senzing-mysql
        export DATABASE_PORT=3306
        export DATABASE_DATABASE=G2
        export SENZING_WEBAPP_PORT=8889
    ```

1. Run the docker container.
   Example:

    ```console
    export SENZING_DATABASE_URL="${DATABASE_PROTOCOL}://${DATABASE_USERNAME}:${DATABASE_PASSWORD}@${DATABASE_HOST}:${DATABASE_PORT}/${DATABASE_DATABASE}"

    sudo docker run \
      --env SENZING_DATABASE_URL="${SENZING_DATABASE_URL}" \
      --interactive \
      --net ${SENZING_NETWORK} \
      --publish ${SENZING_WEBAPP_PORT}:8080 \
      --rm \
      --tty \
      --volume ${SENZING_DATA_VERSION_DIR}:/opt/senzing/data \
      --volume ${SENZING_ETC_DIR}:/etc/opt/senzing \
      --volume ${SENZING_G2_DIR}:/opt/senzing/g2 \
      --volume ${SENZING_VAR_DIR}:/var/opt/senzing \
      senzing/senzing-api-server \
        -concurrency 10 \
        -httpPort 8080 \
        -bindAddr all \
        -iniFile /opt/senzing/g2/python/G2Module.ini
    ```

### Test Docker container

1. Wait for the following message in the terminal showing docker log.

    ```console
    Started Senzing REST API Server on port 8080.

    Server running at:

    http://0.0.0.0/0.0.0.0:8080/
    ```

1. Test Senzing REST API server.
   *Note:* port 8889 on the localhost has been mapped to port 8080 in the docker container.
   See `SENZING_WEBAPP_PORT` definition.
   Example:

    ```console
    export SENZING_API_SERVICE=http://localhost:8889

    curl -X GET ${SENZING_API_SERVICE}/heartbeat
    curl -X GET ${SENZING_API_SERVICE}/license
    curl -X GET ${SENZING_API_SERVICE}/entities/1
    ```

1. To exit, press `control-c` in terminal showing docker log.

## Develop

### Prerequisite software

The following software programs need to be installed:

1. [git](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/install-git.md)
1. [make](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/install-make.md)
1. [docker](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/install-docker.md)

### Clone repository

For more information on environment variables,
see [Environment Variables](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md).

1. Set these environment variable values:

    ```console
    export GIT_ACCOUNT=senzing
    export GIT_REPOSITORY=senzing-api-server
    ```

1. Follow steps in [clone-repository](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/clone-repository.md) to install the Git repository.

1. After the repository has been cloned, be sure the following are set:

    ```console
    export GIT_ACCOUNT_DIR=~/${GIT_ACCOUNT}.git
    export GIT_REPOSITORY_DIR="${GIT_ACCOUNT_DIR}/${GIT_REPOSITORY}"
    ```

### Build docker image for development

1. Find value for `SENZING_G2_JAR_VERSION`.

    ```console
    export SENZING_G2_DIR=/opt/senzing/g2
    cat ${SENZING_G2_DIR}/g2BuildVersion.json
    ```

    or

    ```console
    export SENZING_G2_DIR=/opt/senzing/g2
    cat ${SENZING_G2_DIR}/g2BuildVersion.json | jq --raw-output '.VERSION'
    ```

1. Build docker image.

    - **SENZING_G2_JAR_PATHNAME** - Path to the `g2.jar`. Default: `/opt/senzing/g2/lib/g2.jar`
    - **SENZING_G2_JAR_VERSION** - Version of the `g2.jar` file.

    Example:

    ```console
    cd ${GIT_REPOSITORY_DIR}

    export SENZING_G2_DIR=/opt/senzing/g2
    export SENZING_G2_JAR_PATHNAME=${SENZING_G2_DIR}/lib/g2.jar
    export SENZING_G2_JAR_VERSION=1.11.0

    make docker-build
    ```

    Another example:

    ```console
    cd ${GIT_REPOSITORY_DIR}

    export SENZING_G2_DIR=/opt/senzing/g2

    sudo make \
        SENZING_G2_JAR_PATHNAME=${SENZING_G2_DIR}/lib/g2.jar \
        SENZING_G2_JAR_VERSION=$(cat ${SENZING_G2_DIR}/g2BuildVersion.json | jq --raw-output '.VERSION') \
        docker-build
    ```

## Examples

1. Examples of use:
    1. [docker-compose-stream-loader-kafka-demo](https://github.com/Senzing/docker-compose-stream-loader-kafka-demo)
    1. [kubernetes-demo](https://github.com/Senzing/kubernetes-demo)
    1. [rancher-demo](https://github.com/Senzing/rancher-demo/tree/master/docs/db2-cluster-demo.md)

## Errors

1. See [docs/errors.md](docs/errors.md).

## References
