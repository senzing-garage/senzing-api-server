# senzing-api-server

If you are beginning your journey with [Senzing],
please start with [Senzing Quick Start guides].

You are in the [Senzing Garage] where projects are "tinkered" on.
Although this GitHub repository may help you understand an approach to using Senzing,
it's not considered to be "production ready" and is not considered to be part of the Senzing product.
Heck, it may not even be appropriate for your application of Senzing!

## Overview

The Senzing Rest API Server implemented in Java.

The [Senzing API OAS specification] documents the available API methods, their parameters and the response formats.

### Contents

1. [Demonstrate using Command Line]
   1. [Dependencies]
   1. [Building]
   1. [Running]
   1. [Running with SSL]
1. [Demonstrate using Docker]
   1. [Expectations for docker]
   1. [Configuration]
   1. [External database]
   1. [Database support]
   1. [Run docker container]
   1. [Test docker container]
1. [License]
1. [References]

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

You will also need the Senzing product installation to run the Senzing REST API Server,
but you should not need it to build the server. The Senzing REST API Server should
build (including running of auto tests) without Senzing installed. The notable exception
to that rule is if you want to run the auto tests as end-to-end (E2E) tests using a live
Senzing product installation rather than cached test data (more on that below).

1. Setup your environment. As previously stated, the API's rely on native libraries and
   the environment must be properly setup to find those libraries:

   1. Linux

      ```console
      export SENZING_G2_DIR=/opt/senzing/g2

      export LD_LIBRARY_PATH=${SENZING_G2_DIR}/lib:${SENZING_G2_DIR}/lib/debian:$LD_LIBRARY_PATH
      ```

   2. Windows

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

**NOTE**: If you want to run the automated tests as end-to-end (E2E) tests with a live
Senzing product you can bypass the replay of the cached test data and execute the API's
directly using the following alternate build command:

```console
mvn install -Dcom.senzing.api.test.replay.direct=true
```

The JAR file will be contained in the `target` directory under the name `senzing-api-server-[version].jar`.

Where `[version]` is the version number from the `pom.xml` file.

### Running

To execute the server you will use `java -jar`. It assumed that your environment
is properly configured as described in the "Dependencies" section above.

To start up you must provide the initialization parameters for the Senzing
native API. This is done through **one** of the following options:

- `--init-file` (specifies a path to a JSON file containing the init parameters)
- `--init-json` (specifies the actual JSON text containing the init parameters)
- `--ini-file` (specifies a path to an INI file containing the init parameters)
- `--init-env-var` (specifies an environment variable to read the JSON text from)

The `G2CONFIGFILE` path should normally be excluded from the initialization
parameters to load the default configuration that has been set for the repository.
The `G2CONFIGFILE` parameter referencing a configuration on the file system may
still be specified; however, when starting up this way you do not get auto
reinitialization of the configuration when it changes (i.e.: when the default
configuration changes) and you will be responsible for keeping the configuration
in sync across multiple processes that may be using it and restarting the Senzing
REST API Server manually to refresh the configuration.

Other command-line options may be useful to you as well. Execute

```console
java -jar target/senzing-api-server-3.3.0.jar --help
```

to obtain a help message describing all available options.
For example:

```console
java -jar senzing-api-server-3.3.0.jar <options>

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
        server.  Administrative functions include those that would modify
        the active configuration (e.g.: adding data sources, entity types,
        or entity classes).  The true/false parameter is optional, if not
        specified then true is assumed.  If specified as false then it is
        the same as omitting the option with the exception that omission
        falls back to the environment variable setting whereas an explicit
        false overrides any environment variable.  If not specified then
        administrative functions will return a 403 Forbidden response.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_ENABLE_ADMIN

   --http-port <port-number>
        Also -httpPort.  Sets the port for HTTP communication.  If not
        specified, then the default port (8250) is used.
        Specify 0 for a randomly selected available port number.  This
        option cannot be specified if SSL client authentication is configured.
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
        specified, then the module name defaults to "senzing-api-server".
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

   --debug [true|false]
        Also -debug.  If specified then debug logging is enabled.  The
        true/false parameter is optional, if not specified then true is assumed.
        If specified as false then it is the same as omitting the option with
        the exception that omission falls back to the environment variable
        setting whereas an explicit false overrides any environment variable.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_DEBUG

   --monitor-file <file-path>
        Also -monitorFile.  Specifies a file whose timestamp is monitored to
        determine when to shutdown.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_MONITOR_FILE


[ HTTPS / SSL Options ]
   The following options pertain to HTTPS / SSL configuration.  The
   --key-store and --key-store-password options are the minimum required
   options to enable HTTPS / SSL communication.  If HTTPS / SSL communication
   is enabled, then HTTP communication is disabled UNLESS the --http-port
   option is specified.  However, if client SSL authentication is configured
   via the --client-key-store and --client-key-store-password options then
   enabling HTTP communication via the --http-port option is prohibited.

   --https-port <port-number>
        Also -httpsPort.  Sets the port for secure HTTPS communication.
        While the default HTTPS port is 8263 if not specified,
        HTTPS is only enabled if the --key-store option is specified.
        Specify 0 for a randomly selected available port number.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_SECURE_PORT

   --key-store <path-to-pkcs12-keystore-file>
        Also -keyStore.  Specifies the key store file that holds the private
        key that the sever uses to identify itself for HTTPS communication.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_KEY_STORE

   --key-store-password <password>
        Also -keyStorePassword.  Specifies the password for decrypting the
        key store file specified with the --key-store option.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_KEY_STORE_PASSWORD

   --key-alias <server-key-alias>
        Also -keyAlias.  Optionally specifies the alias for the private server
        key in the specified key store.  If not specified, then the first key
        found in the specified key store is used.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_KEY_ALIAS

   --client-key-store <path-to-pkcs12-keystore-file>
        Also -clientKeyStore.  Specifies the key store file that holds the
        public keys of those clients that are authorized to connect.  If this
        option is specified then SSL client authentication is required and
        the --http-port option is forbidden.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_CLIENT_KEY_STORE

   --client-key-store-password <password>
        Also -clientKeyStorePassword.  Specifies the password for decrypting
        the key store file specified with the --client-key-store option.
        --> VIA ENVIRONMENT: SENZING_API_SERVER_CLIENT_KEY_STORE_PASSWORD


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

### Running with SSL

#### Enabling Basic SSL Support

By default, the Senzing REST API Server will only accept connections on a single
port that supports HTTP communication. The default HTTP port is `8250` and can
be changed via the `--http-port` command-line option. Alternatively, the
Senzing REST API Server can be started with only HTTPS support on a single port
or with both HTTP and HTTPS support (on separate ports).

In order to enable HTTPS the server's private key must be provided in an
encrypted PKCS12 key store via the `--key-store` option. The key store is
decrypted by a password provided via the `--key-store-password` option. If the
key store contains more than one key and a key other than the first should be
used then specific key alias can be provided via the `--key-alias` option. By
providing the server's private key HTTPS is enabled on the default port of
`8263`. The HTTPS port can be configured via the `--https-port` command-line
option.

**NOTE:** By enabling HTTPS, HTTP will be disabled by default. You can enable
both by explicitly providing the `--http-port` option as well.

Let's look at some examples for enabling HTTPS with a self-signed certificate.

##### Example with Java KeyTool fore basic SSL support

1. Create the server PKCS12 key store (`sz-api-server-store.p12`).
   **NOTE:** you will be prompted to provide the 7 fields for the Distinguished Name
   ("DN") for the certificate being generated.

   ```console
   keytool -genkey \
           -alias sz-api-server \
           -keystore sz-api-server-store.p12 \
           -storetype PKCS12 \
           -keyalg RSA \
           -storepass change-it \
           -validity 730 \
           -keysize 2048
   ```

1. Start the Senzing API Server with the server key store:

   ```console
   java -jar senzing-api-server-2.7.0.jar \
             --ini-file /etc/opt/senzing/G2Module.ini \
             --key-store sz-api-server-store.p12 \
             --key-store-password change-it \
             --key-alias sz-api-server
   ```

1. Now let's test it with `curl`. Keep in mind that our certificate is
   self-signed, so we need to use the `-k` option with curl so it does not
   reject the self-signed certificate:

   ```console
   curl -k https://localhost:8263/heartbeat
   ```

1. So far so good, but if you need your application client or browser to
   trust the self-signed certificate you may need to export it to a file
   (`sz-api-server.cer`) and then import to a keychain:

   ```console
   keytool -export \
           -keystore sz-api-server-store.p12 \
           -storepass change-it \
           -storetype PKCS12 \
           -alias sz-api-server \
           -file sz-api-server.cer
   ```

1. If your client application uses PKCS12 key store for its trusted
   certificates then you can add the certificate to a trust store:

   ```console
   keytool -import \
           -file sz-api-server.cer \
           -alias sz-api-server \
           -keystore my-trust-store.p12 \
           -storetype PKCS12 \
           -storepass change-it
   ```

#### SSL Client Authentication

In addition to supporting HTTPS on the server, you can also configure the server
to only accept connections from clients communicating with specific SSL
certificates. If SSL Client Authentication is configured then HTTP support
is not allowed because clients cannot be identified over HTTP, thus the
`--http-port` command-line option is prohibited.

SSL Client authentication is configured by providing an encrypted PKCS12 key
store containing the public keys of the authorized clients via the
`--client-key-store` option. The client key store is decrypted using the
password provided by the `--client-key-store-password` option.

Let's look at some examples for enabling SSL client authentication with a
self-signed certificate.

##### Example with Java KeyTool for SSL Client Authentication

1. We will assume a single authorized client certificate for our example
   purposes. So first, let's create the client key and certificate for the
   client to use. **NOTE:** you will be prompted to provide the 7 fields for
   the Distinguished Name ("DN") for the certificate being generated.

   ```console
   keytool -genkey \
           -alias my-client \
           -keystore my-client-store.p12 \
           -storetype PKCS12 \
           -keyalg RSA \
           -storepass change-it \
           -validity 730 \
           -keysize 2048
   ```

1. Export the client certificate and create a trust store containing it.

   ```console
   keytool -export \
           -keystore my-client-store.p12 \
           -storepass change-it \
           -storetype PKCS12 \
           -alias my-client \
           -file my-client.cer

   keytool -import \
           -file my-client.cer \
           -alias my-client \
           -keystore client-trust-store.p12 \
           -storetype PKCS12 \
           -storepass change-it
   ```

1. Start the Senzing API Server with the server key store (from the previous
   section) and this time we will use the client trust store options.

   ```console
   java -jar senzing-api-server-2.7.0.jar \
             --ini-file /etc/opt/senzing/G2Module.ini \
             --key-store sz-api-server-store.p12 \
             --key-store-password change-it \
             --key-alias sz-api-server \
             --client-key-store client-trust-store.p12 \
             --client-key-store-password change-it
   ```

1. Now let's test it with `curl` and the `-k` option as we did in the
   previous example, but we won't provide the client certificate to `curl`.
   The expectation is that the server will reject the request.

   ```console
   curl -k https://localhost:8263/heartbeat

   > curl: (35) error:1401E412:SSL routines:CONNECT_CR_FINISHED:sslv3 alert bad certificate

   ```

1. Now try `curl` again with the `--cert` and `--cert-type` options to get
   `curl` to authenticate itself with the SSL certificate.

   ```console
   curl -k https://localhost:8263/heartbeat \
        --cert my-client-store.p12:change-it \
        --cert-type P12
   ```

## Demonstrate using Docker

### Expectations for docker

- **Space:** This repository and demonstration require 6 GB free disk space.
- **Time:** Budget 40 minutes to get the demonstration up-and-running, depending on CPU and network speeds.
- **Background knowledge:** This repository assumes a working knowledge of:
  - [Docker]

### Configuration

Configuration values specified by environment variable or command line parameter.

- **[SENZING_API_SERVER_BIND_ADDR]**
- **[SENZING_API_SERVER_PORT]**
- **[SENZING_API_SERVER_ENABLE_ADMIN]**
- **[SENZING_API_SERVER_READ_ONLY]**
- **[SENZING_API_SERVER_CONCURRENCY]**
- **[SENZING_API_SERVER_ALLOWED_ORIGINS]**
- **[SENZING_API_SERVER_MODULE_NAME]**
- **[SENZING_API_SERVER_INI_FILE]**
- **[SENZING_API_SERVER_INIT_FILE]**
- **[SENZING_API_SERVER_INIT_ENV_VAR]**
- **[SENZING_API_SERVER_INIT_JSON]**
- **[SENZING_API_SERVER_CONFIG_ID]**
- **[SENZING_API_SERVER_AUTO_REFRESH_PERIOD]**
- **[SENZING_API_SERVER_STATS_INTERVAL]**
- **[SENZING_API_SERVER_SKIP_STARTUP_PERF]**
- **[SENZING_API_SERVER_VERBOSE]**
- **[SENZING_API_SERVER_QUIET]**
- **[SENZING_API_SERVER_MONITOR_FILE]**
- **[SENZING_SQS_INFO_QUEUE_URL]**
- **[SENZING_RABBITMQ_INFO_HOST]**
- **[SENZING_RABBITMQ_HOST]**
- **[SENZING_RABBITMQ_INFO_PORT]**
- **[SENZING_RABBITMQ_PORT]**
- **[SENZING_RABBITMQ_INFO_USERNAME]**
- **[SENZING_RABBITMQ_USERNAME]**
- **[SENZING_RABBITMQ_INFO_PASSWORD]**
- **[SENZING_RABBITMQ_PASSWORD]**
- **[SENZING_RABBITMQ_INFO_VIRTUAL_HOST]**
- **[SENZING_RABBITMQ_VIRTUAL_HOST]**
- **[SENZING_RABBITMQ_INFO_EXCHANGE]**
- **[SENZING_RABBITMQ_EXCHANGE]**
- **[SENZING_RABBITMQ_INFO_ROUTING_KEY]**
- **[SENZING_KAFKA_INFO_BOOTSTRAP_SERVER]**
- **[SENZING_KAFKA_BOOTSTRAP_SERVER]**
- **[SENZING_KAFKA_INFO_GROUP]**
- **[SENZING_KAFKA_GROUP]**
- **[SENZING_KAFKA_INFO_TOPIC]**
- **[SENZING_DATABASE_URL]**
- **[SENZING_DEBUG]**
- **[SENZING_G2_DIR]**
- **[SENZING_NETWORK]**
- **[SENZING_RUNAS_USER]**

### External database

:thinking: **Optional:** Use if storing data in an external database.
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

:thinking: **Optional:** Some database need additional support.
For other databases, these steps may be skipped.

1. **Db2:** See [Support Db2] instructions to set `SENZING_OPT_IBM_DIR_PARAMETER`.
1. **MS SQL:** See [Support MS SQL] instructions to set `SENZING_OPT_MICROSOFT_DIR_PARAMETER`.

### Run docker container

1. :pencil2: Set environment variables.
   Example:

   ```console
   export SENZING_API_SERVER_PORT=8250
   ```

1. Run docker container.
   Example:

   ```console
   sudo docker run \
     --interactive \
     --publish ${SENZING_API_SERVER_PORT}:8250 \
     --rm \
     --tty \
     ${SENZING_DATABASE_URL_PARAMETER} \
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
   _Note:_ port 8250 on the localhost has been mapped to port 8250 in the docker container.
   See `SENZING_API_SERVER_PORT` definition.
   Example:

   ```console
   export SENZING_API_SERVICE=http://localhost:8250

   curl -X GET ${SENZING_API_SERVICE}/heartbeat
   curl -X GET ${SENZING_API_SERVICE}/license
   curl -X GET ${SENZING_API_SERVICE}/entities/1
   ```

1. To exit, press `control-c` in terminal showing docker log.

## License

View [license information] for the software container in this Docker image.
Note that this license does not permit further distribution.

This Docker image may also contain software from the
[Senzing GitHub community] under the [Apache License 2.0].

Further, as with all Docker images,
this likely also contains other software which may be under other licenses
(such as Bash, etc. from the base distribution,
along with any direct or indirect dependencies of the primary software being contained).

As for any pre-built image usage,
it is the image user's responsibility to ensure that any use of this image complies
with any relevant licenses for all software contained within.

## References

1. [Development]
1. [Errors]
1. [Examples]
1. Related artifacts:
   1. [DockerHub]
   1. [Helm Chart]

[Apache License 2.0]: https://www.apache.org/licenses/LICENSE-2.0
[Building]: #building
[Configuration]: #configuration
[Database support]: #database-support
[Demonstrate using Command Line]: #demonstrate-using-command-line
[Demonstrate using Docker]: #demonstrate-using-docker
[Dependencies]: #dependencies
[Development]: docs/development.md
[Docker]: https://github.com/senzing-garage/knowledge-base/blob/main/WHATIS/docker.md
[DockerHub]: https://hub.docker.com/r/senzing/senzing-api-server
[Errors]: docs/errors.md
[Examples]: docs/examples.md
[Expectations for docker]: #expectations-for-docker
[External database]: #external-database
[Helm Chart]: https://github.com/senzing-garage/charts/tree/main/charts/senzing-api-server
[license information]: https://senzing.com/end-user-license-agreement/
[License]: #license
[References]: #references
[Run docker container]: #run-docker-container
[Running with SSL]: #running-with-ssl
[Running]: #running
[Senzing API OAS specification]: http://editor.swagger.io/?url=https://raw.githubusercontent.com/Senzing/senzing-rest-api/main/senzing-rest-api.yaml
[Senzing Garage]: https://github.com/senzing-garage
[Senzing GitHub community]: https://github.com/Senzing/
[Senzing Quick Start guides]: https://docs.senzing.com/quickstart/
[SENZING_API_SERVER_ALLOWED_ORIGINS]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_api_server_allowed_origins
[SENZING_API_SERVER_AUTO_REFRESH_PERIOD]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_api_server_auto_refresh_period
[SENZING_API_SERVER_BIND_ADDR]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_api_server_bind_addr
[SENZING_API_SERVER_CONCURRENCY]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_api_server_concurrency
[SENZING_API_SERVER_CONFIG_ID]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_api_server_config_id
[SENZING_API_SERVER_ENABLE_ADMIN]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_api_server_enable_admin
[SENZING_API_SERVER_INI_FILE]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_api_server_ini_file
[SENZING_API_SERVER_INIT_ENV_VAR]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_api_server_init_env_var
[SENZING_API_SERVER_INIT_FILE]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_api_server_init_file
[SENZING_API_SERVER_INIT_JSON]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_api_server_init_json
[SENZING_API_SERVER_MODULE_NAME]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_api_server_module_name
[SENZING_API_SERVER_MONITOR_FILE]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_api_server_monitor_file
[SENZING_API_SERVER_PORT]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_api_server_port
[SENZING_API_SERVER_QUIET]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_api_server_quiet
[SENZING_API_SERVER_READ_ONLY]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_api_server_read_only
[SENZING_API_SERVER_SKIP_STARTUP_PERF]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_api_server_skip_startup_perf
[SENZING_API_SERVER_STATS_INTERVAL]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_api_server_stats_interval
[SENZING_API_SERVER_VERBOSE]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_api_server_verbose
[SENZING_DATABASE_URL]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_database_url
[SENZING_DEBUG]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_debug
[SENZING_G2_DIR]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_g2_dir
[SENZING_KAFKA_BOOTSTRAP_SERVER]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_bootstrap_server
[SENZING_KAFKA_GROUP]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_kafka_group
[SENZING_KAFKA_INFO_BOOTSTRAP_SERVER]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_info_bootstrap_server
[SENZING_KAFKA_INFO_GROUP]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_kafka_info_group
[SENZING_KAFKA_INFO_TOPIC]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_kafka_info_topic
[SENZING_NETWORK]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_network
[SENZING_RABBITMQ_EXCHANGE]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_exchange
[SENZING_RABBITMQ_HOST]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_host
[SENZING_RABBITMQ_INFO_EXCHANGE]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_info_exchange
[SENZING_RABBITMQ_INFO_HOST]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_info_host
[SENZING_RABBITMQ_INFO_PASSWORD]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_info_password
[SENZING_RABBITMQ_INFO_PORT]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_info_port
[SENZING_RABBITMQ_INFO_ROUTING_KEY]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_info_routing_key
[SENZING_RABBITMQ_INFO_USERNAME]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_info_username
[SENZING_RABBITMQ_INFO_VIRTUAL_HOST]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_info_virtual_host
[SENZING_RABBITMQ_PASSWORD]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_password
[SENZING_RABBITMQ_PORT]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_port
[SENZING_RABBITMQ_USERNAME]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_username
[SENZING_RABBITMQ_VIRTUAL_HOST]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_virtual_host
[SENZING_RUNAS_USER]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_runas_user
[SENZING_SQS_INFO_QUEUE_URL]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_sqs_info_queue_url
[Senzing]: https://senzing.com/
[Support Db2]: https://github.com/senzing-garage/knowledge-base/blob/main/HOWTO/support-db2.md
[Support MS SQL]: https://github.com/senzing-garage/knowledge-base/blob/main/HOWTO/support-mssql.md
[Test docker container]: #test-docker-container
