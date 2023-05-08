# senzing-api-server development

## Prerequisite software

The following software programs need to be installed:

1. [git](https://github.com/Senzing/knowledge-base/blob/main/WHATIS/git.md)
1. [make](https://github.com/Senzing/knowledge-base/blob/main/WHATIS/make.md)
1. [jq](https://github.com/Senzing/knowledge-base/blob/main/WHATIS/jq.md)
1. [docker](https://github.com/Senzing/knowledge-base/blob/main/WHATIS/docker.md)

## Clone repository

For more information on environment variables,
see [Environment Variables](https://github.com/Senzing/knowledge-base/blob/main/lists/environment-variables.md).

1. Set these environment variable values:

    ```console
    export GIT_ACCOUNT=senzing
    export GIT_REPOSITORY=senzing-api-server
    export GIT_ACCOUNT_DIR=~/${GIT_ACCOUNT}.git
    export GIT_REPOSITORY_DIR="${GIT_ACCOUNT_DIR}/${GIT_REPOSITORY}"
    ```

1. Follow steps in [clone-repository](https://github.com/Senzing/knowledge-base/blob/main/HOWTO/clone-repository.md) to install the Git repository.

## Build docker image for development

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
