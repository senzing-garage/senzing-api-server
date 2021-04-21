# senzing-api-server

## Overview

The Senzing Rest API Server implemented in Java.  The API specification is
defined in by the [Senzing Rest API Proposal](https://github.com/Senzing/senzing-rest-api).

The [Senzing API OAS specification](http://editor.swagger.io/?url=https://raw.githubusercontent.com/Senzing/senzing-rest-api/master/senzing-rest-api.yaml)
documents the available API methods, their parameters and the response formats.

### Related artifacts

1. [DockerHub](https://hub.docker.com/r/senzing/senzing-api-server)
1. [Helm Chart](https://github.com/Senzing/charts/tree/master/charts/senzing-api-server)

### Contents

1. [Demonstrate using Command Line](#demonstrate-using-command-line)
    1. [Dependencies](#dependencies)
    1. [Building](#building)
    1. [Running](#running)
1. [Demonstrate using Docker](#demonstrate-using-docker)
    1. [Expectations for docker](#expectations-for-docker)
    1. [Initialize Senzing](#initialize-senzing)
    1. [Configuration](#configuration)
    1. [Volumes](#volumes)
    1. [Docker network](#docker-network)
    1. [Docker user](#docker-user)
    1. [External database](#external-database)
    1. [Database support](#database-support)
    1. [Run docker container](#run-docker-container)
    1. [Test docker container](#test-docker-container)
1. [Develop](#develop)
    1. [Prerequisite software](#prerequisite-software)
    1. [Clone repository](#clone-repository)
    1. [Build docker image for development](#build-docker-image-for-development)
1. [Examples](#examples)
1. [Errors](#errors)
1. [References](#references)

### Legend

1. :thinking: - A "thinker" icon means that a little extra thinking may be required.
   Perhaps you'll need to make some choices.
   Perhaps it's an optional step.
1. :pencil2: - A "pencil" icon means that the instructions may need modification before performing.
1. :warning: - A "warning" icon means that something tricky is happening, so pay attention.

## Demonstrate using Command Line

### Dependencies

To build the Senzing REST API Server you will need Apache Maven (recommend version 3.6.1 or later)
as well as OpenJDK version 11.0.x (recommend version 11.0.6+10 or later).

You will also need the Senzing `g2.jar` file installed in your Maven repository.
The Senzing REST API Server requires version 2.x or later of the Senzing API and Senzing App.
In order to install `g2.jar` you must:

1. Locate your
   [SENZING_G2_DIR](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_g2_dir)
   directory.
   The default locations are:
    1. [Linux](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/install-senzing-api.md#centos): `/opt/senzing/g2`
    1. Windows MSI Installer: `C:\Program Files\Senzing\`

1. Determine your `SENZING_G2_JAR_VERSION` version number:
    1. Locate your `g2BuildVersion.json` file:
        1. Linux: `${SENZING_G2_DIR}/g2BuildVersion.json`
        1. Windows: `${SENZING_G2_DIR}\data\g2BuildVersion.json`
    1. Find the value for the `"VERSION"` property in the JSON contents.
       Example:

        ```console
        {
           "PLATFORM": "Linux",
           "VERSION": "2.4.1",
           "BUILD_VERSION": "2.4.1.21064",
           "BUILD_NUMBER": "2021_03_05__02_00",
           "DATA_VERSION": "1.0.0"
        }
        ```

1. Install the `g2.jar` file in your local Maven repository, replacing the
   `${SENZING_G2_DIR}` and `${SENZING_G2_JAR_VERSION}` variables as determined above:

    1. Linux:

        ```console
        export SENZING_G2_DIR=/opt/senzing/g2
        export SENZING_G2_JAR_VERSION=2.4.1

        mvn install:install-file \
            -Dfile=${SENZING_G2_DIR}/lib/g2.jar \
            -DgroupId=com.senzing \
            -DartifactId=g2 \
            -Dversion=${SENZING_G2_JAR_VERSION} \
            -Dpackaging=jar
        ```

    1. Windows:

        ```console
        set SENZING_G2_DIR="C:\Program Files\Senzing\g2"
        set SENZING_G2_JAR_VERSION=2.4.1

        mvn install:install-file \
            -Dfile="%SENZING_G2_DIR%\lib\g2.jar" \
            -DgroupId=com.senzing \
            -DartifactId=g2 \
            -Dversion="%SENZING_G2_JAR_VERSION%" \
            -Dpackaging=jar
        ```

1. Setup your environment.  The API's rely on native libraries and the
   environment must be properly setup to find those libraries:

    1. Linux

        ```console
        export SENZING_G2_DIR=/opt/senzing/g2

        export LD_LIBRARY_PATH=${SENZING_G2_DIR}/lib:${SENZING_G2_DIR}/lib/debian:$LD_LIBRARY_PATH
        ```

    1. Windows

        ```console
        set SENZING_G2_DIR="C:\Program Files\Senzing\g2"

        set Path=%SENZING_G2_DIR%\lib;%Path%
        ```

1. Ensure the OpenAPI specification GIT submodule (senzing-rest-api-specification) is cloned:

    ```console
    git submodule update --init --recursive
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

To start up you must provide the initialization parameters for the Senzing
native API.  This is done through one of: `-initFile`, `-initEnvVar` or the
`-initJson` options to specify how to obtain the initialization JSON parameters.
The `G2CONFIGFILE` path is excluded from the initialization parameters in favor
of loading the default configuration that has been set for the repository.

The deprecated `-iniFile` option can also be used to startup with a deprecated
INI file with a `G2CONFIGFILE` parameter referencing a configuration on the
file system.  However, when starting up this way you do not get auto
reinitialization of the configuration when it changes (i.e.: when the default
configuration changes) and you will be responsible for keeping the configuration
in sync across multiple processes that may be using it.

Other command-line options may be useful to you as well.  Execute

```console
java -jar target/senzing-api-server-2.6.0.jar --help
```

to obtain a help message describing all available options.
For example:

```console
$ java -jar target/senzing-api-server-2.6.0.jar --help

java -jar senzing-api-server-2.6.0.jar <options>

<options> includes: 

[ Standard Options ]

   --help
        Also -help.  Should be the first and only option if provided.
        Causes this help message to be displayed.
        NOTE: If this option is provided, the server will not start.

   --version
        Also -version.  Should be the first and only option if provided.
        Causes the version of the G2 REST API Server to be displayed.
        NOTE: If this option is provided, the server will not start.

   --read-only [true|false]
        Also -readOnly.  Disables functions that would modify the entity
        repository data, causing those functions to return a 403 Forbidden
        response.  The true/false parameter is optional, if not specified
        then true is assumed.  If specified as false then it is the same as
        omitting the option with the exception that omission falls back to the
        environment variable setting whereas an explicit false overrides any
        environment variable.  NOTE: this option will not only disable loading
        data to the entity repository, but will also disable modifications to
        the configuration even if the --enable-admin option is provided.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_READ_ONLY

   --enable-admin [true|false]
        Also -enableAdmin.  Enables administrative functions via the API
        server.  The true/false parameter is optional, if not specified then
        true is assumed.  If specified as false then it is the same as omitting
        the option with the exception that omission falls back to the
        environment variable setting whereas an explicit false overrides any
        environment variable.  If not specified then administrative functions
        will return a 403 Forbidden response.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_ENABLE_ADMIN

   --http-port <port-number>
        Also -httpPort.  Sets the port for HTTP communication.  If not
        specified, then the default port (2080) is used.
        Specify 0 for a randomly selected available port number.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_PORT

   --bind-addr <ip-address|loopback|all>
        Also -bindAddr.  Sets the bind address for HTTP communication.  If not
        provided the bind address defaults to the loopback address.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_BIND_ADDR

   --url-base-path <base-path>
        Also -urlBasePath.  Sets the URL base path for the API Server.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_URL_BASE_PATH

   --allowed-origins <url-domain>
        Also -allowedOrigins.  Sets the CORS Access-Control-Allow-Origin header
        for all endpoints.  There is no default value.  If not specified then
        the Access-Control-Allow-Origin is not included with responses.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_ALLOWED_ORIGINS

   --concurrency <thread-count>
        Also -concurrency.  Sets the number of threads available for executing 
        Senzing API functions (i.e.: the number of engine threads).
        If not specified, then this defaults to 8.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_CONCURRENCY

   --http-concurrency <thread-count>
        Also -httpConcurrency.  Sets the maximum number of threads available
        for the HTTP server.  The single parameter to this option should be
        a positive integer.  If not specified, then this defaults to 200.  If
        the specified thread count is less than 10 then an error is reported
        --> VIA ENVIRONMENT: SENZING_API_SERVER_HTTP_CONCURRENCY

   --module-name <module-name>
        Also -moduleName.  The module name to initialize with.  If not
        specified, then the module name defaults to "SzApiServer".
        --> VIA ENVIRONMENT: SENZING_API_SERVER_MODULE_NAME

   --ini-file <ini-file-path>
        Also -iniFile.  The path to the Senzing INI file to with which to
        initialize.
        EXAMPLE: -iniFile /etc/opt/senzing/G2Module.ini
        --> VIA ENVIRONMENT: SENZING_API_SERVER_INI_FILE

   --init-file <json-init-file>
        Also -initFile.  The path to the file containing the JSON text to
        use for Senzing initialization.
        EXAMPLE: -initFile ~/senzing/g2-init.json
        --> VIA ENVIRONMENT: SENZING_API_SERVER_INIT_FILE

   --init-env-var <environment-variable-name>
        Also -initEnvVar.  The environment variable from which to extract
        the JSON text to use for Senzing initialization.
        *** SECURITY WARNING: If the JSON text contains a password
        then it may be visible to other users via process monitoring.
        EXAMPLE: -initEnvVar SENZING_INIT_JSON
        --> VIA ENVIRONMENT: SENZING_API_SERVER_INIT_ENV_VAR

   --init-json <json-init-text>
        Also -initJson.  The JSON text to use for Senzing initialization.
        *** SECURITY WARNING: If the JSON text contains a password
        then it may be visible to other users via process monitoring.
        EXAMPLE: -initJson "{"PIPELINE":{ ... }}"
        --> VIA ENVIRONMENT: SENZING_API_SERVER_INIT_JSON

   --config-id <config-id>
        Also -configId.  Use with the -iniFile, -initFile, -initEnvVar or
        -initJson options to force a specific configuration ID to use for
        initialization.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_CONFIG_ID

   --auto-refresh-period <positive-integer-seconds|0|negative-integer>
        Also -autoRefreshPeriod.  If leveraging the default configuration
        stored in the database, this is used to specify how often the API
        server should background check that the current active config is the
        same as the current default config, and if different reinitialize
        with the current default config.  If zero is specified, then the
        auto-refresh is disabled and it will only occur when a requested
        configuration element is not found in the current active config.
        Specifying a negative integer is allowed but is used to enable a
        check and conditional refresh only when manually requested
        (programmatically).  NOTE: This is option ignored if auto-refresh is
        disabled because the config was specified via the G2CONFIGFILE init
        option or if --config-id has been specified to lock to a specific
        configuration.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_AUTO_REFRESH_PERIOD

   --stats-interval <milliseconds>
        Also -statsInterval.  The minimum number of milliseconds between
        logging of stats.  This is minimum because stats logging is suppressed
        if the API Server is idle or active but not performing activities
        pertaining to entity scoring.  In such cases, stats logging is delayed
        until an activity pertaining to entity scoring is performed.  By
        default this is set to the millisecond equivalent of 15 minutes.  If
        zero (0) is specified then the logging of stats will be suppressed.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_STATS_INTERVAL

   --skip-startup-perf [true|false]
        Also -skipStartupPerf.  If specified then the performance check on
        startup is skipped.  The true/false parameter is optional, if not
        specified then true is assumed.  If specified as false then it is the
        same as omitting the option with the exception that omission falls back
        to the environment variable setting whereas an explicit false overrides
        any environment variable.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_SKIP_STARTUP_PERF

   --skip-engine-priming [true|false]
        Also -skipEnginePriming.  If specified then the API Server will not
        prime the engine on startup.  The true/false parameter is optional, if
        not specified then true is assumed.  If specified as false then it is
        the same as omitting the option with the exception that omission falls
        back to the environment variable setting whereas an explicit false
        overrides any environment variable.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_SKIP_ENGINE_PRIMING

   --verbose [true|false]
        Also -verbose.  If specified then initialize in verbose mode.  The
        true/false parameter is optional, if not specified then true is assumed.
        If specified as false then it is the same as omitting the option with
        the exception that omission falls back to the environment variable
        setting whereas an explicit false overrides any environment variable.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_VERBOSE

   --quiet [true|false]
        Also -quiet.  If specified then the API server reduces the number of
        messages provided as feedback to standard output.  This applies only to
        messages generated by the API server and not by the underlying API
        which can be quite prolific if --verbose is provided.  The true/false
        parameter is optional, if not specified then true is assumed.  If
        specified as false then it is the same as omitting the option with
        the exception that omission falls back to the environment variable
        setting whereas an explicit false overrides any environment variable.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_QUIET

   --monitor-file <file-path>
        Also -monitorFile.  Specifies a file whose timestamp is monitored to
        determine when to shutdown.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_MONITOR_FILE

[ Asynchronous Info Queue Options ]
   The following options pertain to configuring an asynchronous message
   queue on which to send "info" messages generated when records are
   loaded, deleted or entities are re-evaluated.  At most one such queue
   can be configured.  If an "info" queue is configured then every load,
   delete and re-evaluate operation is performed with the variant to
   generate an info message.  The info messages that are sent on the queue
   (or topic) are the relevant "raw data" JSON segments.

   --sqs-info-url <url>
        Also -sqsInfoUrl.  Specifies an Amazon SQS queue URL as the info queue.
        --> VIA ENVIRONMENT: SENZING_SQS_INFO_QUEUE_URL

   --rabbit-info-host <hostname>
        Also -rabbitInfoHost.  Used to specify the hostname for connecting to
        RabbitMQ as part of specifying a RabbitMQ info queue.
        --> VIA ENVIRONMENT: SENZING_RABBITMQ_INFO_HOST
                             SENZING_RABBITMQ_HOST (fallback)

   --rabbit-info-port <port>
        Also -rabbitInfoPort.  Used to specify the port number for connecting
        to RabbitMQ as part of specifying a RabbitMQ info queue.
        --> VIA ENVIRONMENT: SENZING_RABBITMQ_INFO_PORT
                             SENZING_RABBITMQ_PORT (fallback)

   --rabbit-info-user <user name>
        Also -rabbitInfoUser.  Used to specify the user name for connecting to
        RabbitMQ as part of specifying a RabbitMQ info queue.
        --> VIA ENVIRONMENT: SENZING_RABBITMQ_INFO_USERNAME
                             SENZING_RABBITMQ_USERNAME (fallback)

   --rabbit-info-password <password>
        Also -rabbitInfoPassword.  Used to specify the password for connecting
        to RabbitMQ as part of specifying a RabbitMQ info queue.
        --> VIA ENVIRONMENT: SENZING_RABBITMQ_INFO_PASSWORD
                             SENZING_RABBITMQ_PASSWORD (fallback)

   --rabbit-info-virtual-host <virtual host>
        Also -rabbitInfoVirtualHost.  Used to specify the virtual host for
        connecting to RabbitMQ as part of specifying a RabbitMQ info queue.
        --> VIA ENVIRONMENT: SENZING_RABBITMQ_INFO_VIRTUAL_HOST
                             SENZING_RABBITMQ_VIRTUAL_HOST (fallback)

   --rabbit-info-exchange <exchange>
        Also -rabbitInfoExchange.  Used to specify the exchange for connecting
        to RabbitMQ as part of specifying a RabbitMQ info queue.
        --> VIA ENVIRONMENT: SENZING_RABBITMQ_INFO_EXCHANGE
                             SENZING_RABBITMQ_EXCHANGE (fallback)

   --rabbit-info-routing-key <routing key>
        Also -rabbitInfoRoutingKey.  Used to specify the routing key for
        connecting to RabbitMQ as part of specifying a RabbitMQ info queue.
        --> VIA ENVIRONMENT: SENZING_RABBITMQ_INFO_ROUTING_KEY

   --kafka-info-bootstrap-server <bootstrap servers>
        Also -kafkaInfoBootstrapServer.  Used to specify the bootstrap servers
        for connecting to Kafka as part of specifying a Kafka info topic.
        --> VIA ENVIRONMENT: SENZING_KAFKA_INFO_BOOTSTRAP_SERVER
                             SENZING_KAFKA_BOOTSTRAP_SERVER (fallback)

   --kafka-info-group <group id>
        Also -kafkaInfoGroupId.  Used to specify the group ID for connecting to
        Kafka as part of specifying a Kafka info topic.
        --> VIA ENVIRONMENT: SENZING_KAFKA_INFO_GROUP
                             SENZING_KAFKA_GROUP (fallback)

   --kafka-info-topic <topic>
        Also -kafkaInfoTopic.  Used to specify the topic name for connecting to
        Kafka as part of specifying a Kafka info topic.
        --> VIA ENVIRONMENT: SENZING_KAFKA_INFO_TOPIC

[ Advanced Options ]

   --config-mgr [config manager options]...
        Also --configmgr.  Should be the first option if provided.  All
        subsequent options are interpreted as configuration manager options.
        If this option is specified by itself then a help message on
        configuration manager options will be displayed.
        NOTE: If this option is provided, the server will not start.

```

If you wanted to run the server on port 8080 and bind to all
network interfaces with a concurrency of 16 you would use:

```console
java -jar target/senzing-api-server-[version].jar \
  --concurrency 16 \
  --http-port 8080 \
  --bind-addr all \
  --init-file ~/senzing/data/g2-init.json
```

## Demonstrate using Docker

### Expectations for docker

#### Space for docker

This repository and demonstration require 6 GB free disk space.

#### Time for docker

Budget 40 minutes to get the demonstration up-and-running, depending on CPU and network speeds.

#### Background knowledge for docker

This repository assumes a working knowledge of:

1. [Docker](https://github.com/Senzing/knowledge-base/blob/master/WHATIS/docker.md)

### Initialize Senzing

1. If Senzing has not been initialized, visit
   "[How to initialize Senzing with Docker](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/initialize-senzing-with-docker.md)".

### Configuration

Configuration values specified by environment variable or command line parameter.

- **[SENZING_API_SERVER_BIND_ADDR](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_API_SERVER_PORT](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_API_SERVER_ENABLE_ADMIN](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_API_SERVER_READ_ONLY](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_API_SERVER_CONCURRENCY](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_API_SERVER_ALLOWED_ORIGINS](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_API_SERVER_MODULE_NAME](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_API_SERVER_INI_FILE](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_API_SERVER_INIT_FILE](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_API_SERVER_INIT_ENV_VAR](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_API_SERVER_INIT_JSON](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_API_SERVER_CONFIG_ID](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_API_SERVER_AUTO_REFRESH_PERIOD](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_API_SERVER_STATS_INTERVAL](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_API_SERVER_SKIP_STARTUP_PERF](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_API_SERVER_VERBOSE](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_API_SERVER_QUIET](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_API_SERVER_MONITOR_FILE](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_SQS_INFO_QUEUE_URL](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_RABBITMQ_INFO_HOST](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_RABBITMQ_HOST](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_RABBITMQ_INFO_PORT](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_RABBITMQ_PORT](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_RABBITMQ_INFO_USERNAME](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_RABBITMQ_USERNAME](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_RABBITMQ_INFO_PASSWORD](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_RABBITMQ_PASSWORD](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_RABBITMQ_INFO_VIRTUAL_HOST](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_RABBITMQ_VIRTUAL_HOST](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_RABBITMQ_INFO_EXCHANGE](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_RABBITMQ_EXCHANGE](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_RABBITMQ_INFO_ROUTING_KEY](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_KAFKA_INFO_BOOTSTRAP_SERVER](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_KAFKA_BOOTSTRAP_SERVER](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_KAFKA_INFO_GROUP](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_KAFKA_GROUP](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_KAFKA_INFO_TOPIC](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_api_service_port)**
- **[SENZING_DATA_VERSION_DIR](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_data_version_dir)**
- **[SENZING_DATABASE_URL](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_database_url)**
- **[SENZING_DEBUG](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_debug)**
- **[SENZING_ETC_DIR](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_etc_dir)**
- **[SENZING_G2_DIR](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_g2_dir)**
- **[SENZING_NETWORK](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_network)**
- **[SENZING_RUNAS_USER](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_runas_user)**
- **[SENZING_VAR_DIR](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md#senzing_var_dir)**

### Volumes

1. :pencil2: Specify the directory containing the Senzing installation.
   Use the same `SENZING_VOLUME` value used when performing
   "[How to initialize Senzing with Docker](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/initialize-senzing-with-docker.md)".
   Example:

    ```console
    export SENZING_VOLUME=/opt/my-senzing
    ```

    1. Here's a simple test to see if `SENZING_VOLUME` is correct.
       The following commands should return file contents.
       Example:

        ```console
        cat ${SENZING_VOLUME}/g2/g2BuildVersion.json
        cat ${SENZING_VOLUME}/data/1.0.0/libpostal/data_version
        ```

    1. :warning:
       **macOS** - [File sharing](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/share-directories-with-docker.md#macos)
       must be enabled for `SENZING_VOLUME`.
    1. :warning:
       **Windows** - [File sharing](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/share-directories-with-docker.md#windows)
       must be enabled for `SENZING_VOLUME`.

1. Identify the `data_version`, `etc`, `g2`, and `var` directories.
   Example:

    ```console
    export SENZING_DATA_VERSION_DIR=${SENZING_VOLUME}/data/1.0.0
    export SENZING_ETC_DIR=${SENZING_VOLUME}/etc
    export SENZING_G2_DIR=${SENZING_VOLUME}/g2
    export SENZING_VAR_DIR=${SENZING_VOLUME}/var
    ```

### Docker network

:thinking: **Optional:**  Use if docker container is part of a docker network.

1. List docker networks.
   Example:

    ```console
    sudo docker network ls
    ```

1. :pencil2: Specify docker network.
   Choose value from NAME column of `docker network ls`.
   Example:

    ```console
    export SENZING_NETWORK=*nameofthe_network*
    ```

1. Construct parameter for `docker run`.
   Example:

    ```console
    export SENZING_NETWORK_PARAMETER="--net ${SENZING_NETWORK}"
    ```

### Docker user

:thinking: **Optional:**  The docker container runs as "USER 1001".
Use if a different userid (UID) is required.

1. :pencil2: Manually identify user.
   User "0" is root.
   Example:

    ```console
    export SENZING_RUNAS_USER="0"
    ```

   Another option, use current user.
   Example:

    ```console
    export SENZING_RUNAS_USER=$(id -u)
    ```

1. Construct parameter for `docker run`.
   Example:

    ```console
    export SENZING_RUNAS_USER_PARAMETER="--user ${SENZING_RUNAS_USER}"
    ```

### External database

:thinking: **Optional:**  Use if storing data in an external database.
If not specified, the internal SQLite database will be used.

1. :pencil2: Specify database.
   Example:

    ```console
    export DATABASE_PROTOCOL=postgresql
    export DATABASE_USERNAME=postgres
    export DATABASE_PASSWORD=postgres
    export DATABASE_HOST=senzing-postgresql
    export DATABASE_PORT=5432
    export DATABASE_DATABASE=G2
    ```

1. Construct Database URL.
   Example:

    ```console
    export SENZING_DATABASE_URL="${DATABASE_PROTOCOL}://${DATABASE_USERNAME}:${DATABASE_PASSWORD}@${DATABASE_HOST}:${DATABASE_PORT}/${DATABASE_DATABASE}"
    ```

1. Construct parameter for `docker run`.
   Example:

    ```console
    export SENZING_DATABASE_URL_PARAMETER="--env SENZING_DATABASE_URL=${SENZING_DATABASE_URL}"
    ```

### Database support

:thinking: **Optional:**  Some database need additional support.
For other databases, these steps may be skipped.

1. **Db2:** See
   [Support Db2](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/support-db2.md)
   instructions to set `SENZING_OPT_IBM_DIR_PARAMETER`.
1. **MS SQL:** See
   [Support MS SQL](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/support-mssql.md)
   instructions to set `SENZING_OPT_MICROSOFT_DIR_PARAMETER`.

### Run docker container

1. :pencil2: Set environment variables.
   Example:

    ```console
    export SENZING_API_SERVICE_PORT=8250
    ```

1. Run docker container.
   Example:

    ```console
    sudo docker run \
      --interactive \
      --publish ${SENZING_API_SERVICE_PORT}:8250 \
      --rm \
      --tty \
      --volume ${SENZING_DATA_VERSION_DIR}:/opt/senzing/data \
      --volume ${SENZING_ETC_DIR}:/etc/opt/senzing \
      --volume ${SENZING_G2_DIR}:/opt/senzing/g2 \
      --volume ${SENZING_VAR_DIR}:/var/opt/senzing \
      ${SENZING_RUNAS_USER_PARAMETER} \
      ${SENZING_DATABASE_URL_PARAMETER} \
      ${SENZING_NETWORK_PARAMETER} \
      ${SENZING_OPT_IBM_DIR_PARAMETER} \
      ${SENZING_OPT_MICROSOFT_DIR_PARAMETER} \
      senzing/senzing-api-server \
        -allowedOrigins "*" \
        -bindAddr all \
        -concurrency 10 \
        -httpPort 8250 \
        -iniFile /etc/opt/senzing/G2Module.ini
    ```

### Test Docker container

1. Wait for the following message in the terminal showing docker log.

    ```console
    Started Senzing REST API Server on port 8250.

    Server running at:

    http://0.0.0.0/0.0.0.0:8250/
    ```

1. Test Senzing REST API server.
   *Note:* port 8250 on the localhost has been mapped to port 8250 in the docker container.
   See `SENZING_API_SERVICE_PORT` definition.
   Example:

    ```console
    export SENZING_API_SERVICE=http://localhost:8250

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
1. [jq](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/install-jq.md)
1. [docker](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/install-docker.md)

### Clone repository

For more information on environment variables,
see [Environment Variables](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md).

1. Set these environment variable values:

    ```console
    export GIT_ACCOUNT=senzing
    export GIT_REPOSITORY=senzing-api-server
    export GIT_ACCOUNT_DIR=~/${GIT_ACCOUNT}.git
    export GIT_REPOSITORY_DIR="${GIT_ACCOUNT_DIR}/${GIT_REPOSITORY}"
    ```

1. Follow steps in [clone-repository](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/clone-repository.md) to install the Git repository.

### Build docker image for development

1. :pencil2: Set environment variables.
   Example:

    ```console
    export SENZING_G2_DIR=/opt/senzing/g2
    export SENZING_API_SERVER_GIT_TAG=0.0.0
    ```

1. Build docker image.
   Example:

    ```console
    cd ${GIT_REPOSITORY_DIR}

    git checkout ${SENZING_API_SERVER_GIT_TAG}
    git submodule update --init --recursive

    sudo make \
        SENZING_G2_JAR_PATHNAME=${SENZING_G2_DIR}/lib/g2.jar \
        SENZING_G2_JAR_VERSION=$(cat ${SENZING_G2_DIR}/g2BuildVersion.json | jq --raw-output '.VERSION') \
        docker-build \
        > make-output.txt
    ```

1. Review output.
   Example:

    ```console
    tail -f ${GIT_REPOSITORY_DIR}/make-output.txt
    ```

1. Note: `sudo make docker-build-development-cache` can be used to create cached docker layers.

## Examples

1. Examples of use:
    1. [docker-compose-stream-loader-kafka-demo](https://github.com/Senzing/docker-compose-stream-loader-kafka-demo)
    1. [kubernetes-demo](https://github.com/Senzing/kubernetes-demo)
    1. [rancher-demo](https://github.com/Senzing/rancher-demo/tree/master/docs/db2-cluster-demo.md)

## Errors

1. See [docs/errors.md](docs/errors.md).

## References
