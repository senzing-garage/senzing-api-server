# senzing-api-server

## Overview

The Senzing Rest API Server implemented in Java.  The API specification is
defined in by the [Senzing Rest API Proposal](https://github.com/Senzing/rest-api-proposal).

The [Senzing API OAS specification](http://editor.swagger.io/?url=https://raw.githubusercontent.com/Senzing/rest-api-proposal/master/senzing-api.yaml)
documents the available API methods, their parameters and the response formats.

### Contents

1. [Using Command Line](#using-command-line)
    1. [Dependencies](#dependencies)
    1. [Building](#building)
    1. [Running](#running)
1. [Using Docker](#using-docker)
    1. [Expectations for docker](#expectations-for-docker)
    1. [Clone repository](#clone-repository)
    1. [Create SENZING_DIR](#create-senzing_dir)
    1. [Build docker image](#build-docker-image)
    1. [Configuration](#configuration)
    1. [Run docker image](#run-docker-image)
    1. [Test docker container](#test-docker-container)
1. [Errors](errors)

## Using Command Line

### Dependencies

To build the Senzing REST API Server you will need Apache Maven (recommend version 3.5.4 or later)
as well as Java 1.8.x (recommend version 1.8.0_171 or later).

You will also need the Senzing "g2.jar" file installed in your Maven repository.
The Senzing REST API Server is being developed in concert with version 1.5.x of
the Senzing API and Senzing App, but will also work with the previously released
version 1.4.x.  In order to install g2.jar you must:

 1) Locate your [`${SENZING_DIR}` directory](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/create-senzing-dir.md).
    The default locations are:
    * Linux Archive Extraction: `/opt/senzing/` (see [Install Instructions](https://github.com/Senzing/hello-senzing-springboot-java/blob/master/doc/debian-based-installation.md#install))
    * Windows MSI Installer: `C:\Program Files\Senzing\`

 2) Determine your `${SENZING_VERSION}` version number:
    * Locate your `g2BuildVersion.json` file:
        * Linux: `${SENZING_DIR}/g2/data/g2BuildVersion.json`
        * Windows: `${SENZING_DIR}\g2\data\g2BuildVersion.json`
    * Find the value for the `"VERSION"` property in the JSON contents.
      For example:

        ```console
        {
            "PLATFORM": "Linux",
            "VERSION": "1.5.19022",
            "BUILD_NUMBER": "2019_01_22__02_00"
        }
        ```

 3) Install the g2.jar file in your local Maven repository, replacing the
    `${SENZING_DIR}` and `${SENZING_VERSION}` variables as determined above:

     * Linux:

       ```console
             export SENZING_DIR=/opt/senzing
             export SENZING_VERSION=1.5.19022

             mvn install:install-file \
                 -Dfile=${SENZING_DIR}/g2/lib/g2.jar \
                 -DgroupId=com.senzing \
                 -DartifactId=g2 \
                 -Dversion=${SENZING_VERSION} \
                 -Dpackaging=jar
       ```

     * Windows:

       ```console
             set SENZING_DIR="C:\Program Files\Senzing"
             set SENZING_VERSION=1.5.19022

             mvn install:install-file \
                 -Dfile=%SENZING_DIR%\g2\lib\g2.jar \
                 -DgroupId=com.senzing \
                 -DartifactId=g2 \
                 -Dversion=%SENZING_VERSION% \
                 -Dpackaging=jar
       ```

 4) Setup your environment.  The API's rely on native libraries and the
    environment must be properly setup to find those libraries:

    * Linux

       ```console
          export SENZING_DIR=/opt/senzing

          export LD_LIBRARY_PATH=${SENZING_DIR}/g2/lib:${SENZING_DIR}/g2/lib/debian:$LD_LIBRARY_PATH
       ```

    * Windows

      ```console
          set SENZING_DIR="C:\Program Files\Senzing"

          set Path=%SENZING_DIR%\g2\lib;$Path
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
`java -jar target/senzing-api-server-1.5.1.jar -help` to obtain a help message
describing all available options.  For example:

  ```console
     cd target

    $ java -jar senzing-api-server-1.5.1.jar -help

    java -jar senzing-api-server-1.5.1.jar <options>

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

For example, if you wanted to run the server on port 8080 and bind to all
network interfaces with a concurrency of 16 you would use:

  ```console
     java -jar target/senzing-api-server-[version].jar \
        -concurrency 16 \
        -httpPort 8080 \
        -bindAddr all \
        -iniFile /opt/senzing/g2/python/G2Module.ini
  ```

## Using Docker

### Expectations for docker

#### Space for docker

1. 6 GB of free disk space are required for using docker to build and demonstrate.

#### Time for docker

1. Budget 40 minutes to create and use docker images, depending on CPU and network speeds.

#### Background knowledge for docker

This task assumes a working knowledge of:

1. [Docker](https://github.com/Senzing/knowledge-base/blob/master/WHATIS/docker.md)

### Clone repository

1. Set these environment variable values:

    ```console
    export GIT_ACCOUNT=senzing
    export GIT_REPOSITORY=senzing-api-server
    ```

   Then follow steps in [clone-repository](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/clone-repository.md).

1. After the repository has been cloned, be sure the following are set:

    ```console
    export GIT_ACCOUNT_DIR=~/${GIT_ACCOUNT}.git
    export GIT_REPOSITORY_DIR="${GIT_ACCOUNT_DIR}/${GIT_REPOSITORY}"
    ```

### Create SENZING_DIR

If you do not already have an `/opt/senzing` directory on your local system, visit
[HOWTO - Create SENZING_DIR](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/create-senzing-dir.md).

### Build docker image

1. Find value for `SENZING_G2_JAR_VERSION`.

    ```console
    cat ${SENZING_DIR}/g2/data/g2BuildVersion.json
    ```

    or  

    ```console
    cat ${SENZING_DIR}/g2/data/g2BuildVersion.json | jq --raw-output '.VERSION'
    ```

1. Build Jar file.

    * **SENZING_G2_JAR_PATHNAME** - Path to the `g2.jar`. Default: `/opt/senzing/g2/lib/g2.jar`
    * **SENZING_G2_JAR_VERSION** - Version of the `g2.jar` file.

    Example:

    ```console
    cd ${GIT_REPOSITORY_DIR}

    export SENZING_G2_JAR_PATHNAME=/opt/senzing/g2/lib/g2.jar
    export SENZING_G2_JAR_VERSION=1.4.18354

    sudo make docker-package
    ```

    Another example:

    ```console
    cd ${GIT_REPOSITORY_DIR}

    export SENZING_G2_JAR_PATHNAME=${SENZING_DIR}/g2/lib/g2.jar
    export SENZING_G2_JAR_VERSION=$(cat ${SENZING_DIR}/g2/data/g2BuildVersion.json | jq --raw-output '.VERSION')

    sudo make docker-package
    ```

    1. Jar file will be in the "target" directory. Example: `${GIT_REPOSITORY_DIR}/target/senzing-api-server-M.m.P.jar`

1. Build docker image.
   *Note:* The following command expects a JAR file at `${GIT_REPOSITORY_DIR}/target/senzing-api-server-M.m.P.jar`.

    ```console
    cd ${GIT_REPOSITORY_DIR}

    sudo make docker-build
    ```

### Configuration

* **SENZING_BIND_ADDR** -
  Port for HTTP bind address communication. Values: ip-address, loopback, all. Default: loopback
* **SENZING_CONCURRENCY** -
  Number of threads available for executing Senzing API functions. Default: 8
* **SENZING_DIR** -
  Location of Senzing libraries. Default: "/opt/senzing".
* **SENZING_INI_FILE** -
  The path to the Senzing INI file to with which to initialize. Default: ${SENZING_DIR}/g2/python/G2Module.ini
* **SENZING_OPTIONS** -
  Additional options for the program.
* **WEBAPP_PORT** -
  Port used by service.  

### Run docker image

#### Variation 1

1. Run the docker image. Example:

    ```console
    export SENZING_DIR=/opt/senzing
    export SENZING_CONCURRENCY=10
    export WEBAPP_PORT=8889

    sudo docker run -it \
      --volume ${SENZING_DIR}:/opt/senzing \
      --publish ${WEBAPP_PORT}:8080 \
      --env SENZING_CONCURRENCY="${SENZING_CONCURRENCY}" \
      senzing/senzing-api-server
    ```

#### Variation 2

1. Run docker image in docker network.

    1. Determine docker network:

        ```console
        docker network ls

        # Choose value from NAME column of docker network ls
        export SENZING_NETWORK=nameofthe_network
        ```

    1. Run the docker image. Example:

        ```console
        export SENZING_DIR=/opt/senzing
        export WEBAPP_PORT=8889

        sudo docker run -it \
          --volume ${SENZING_DIR}:/opt/senzing \
          --net ${SENZING_NETWORK} \
          --publish ${WEBAPP_PORT}:8080 \
          senzing/senzing-api-server
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
   See `WEBAPP_PORT` definition.
   Example:

    ```console
    export SENZING_API_SERVICE=http://localhost:8889

    curl -X GET ${SENZING_API_SERVICE}/heartbeat
    curl -X GET ${SENZING_API_SERVICE}/license
    ```

1. To exit, press `control-c` in terminal showing docker log.

## Errors

1. See [docs/errors.md](docs/errors.md).