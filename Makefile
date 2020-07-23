# PROGRAM_NAME is the name of the GIT repository.
# It should match <artifactId> in pom.xml
PROGRAM_NAME := $(shell basename `git rev-parse --show-toplevel`)

# User variables.

SENZING_G2_JAR_PATHNAME ?= /opt/senzing/g2/lib/g2.jar
SENZING_G2_JAR_VERSION ?= unknown

# Information from git.

GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
GIT_REPOSITORY_NAME := $(shell basename `git rev-parse --show-toplevel`)
GIT_SHA := $(shell git log --pretty=format:'%H' -n 1)
GIT_TAG ?= $(shell git describe --always --tags | awk -F "-" '{print $$1}')
GIT_TAG_END ?= HEAD
GIT_VERSION := $(shell git describe --always --tags --long --dirty | sed -e 's/\-0//' -e 's/\-g.......//')
GIT_VERSION_LONG := $(shell git describe --always --tags --long --dirty)

# Docker.

BASE_IMAGE ?= senzing/senzing-base:1.5.2
BASE_BUILDER_IMAGE ?= senzing/base-image-debian:1.0.3
DOCKER_IMAGE_PACKAGE := $(GIT_REPOSITORY_NAME)-package:$(GIT_VERSION)
DOCKER_IMAGE_TAG ?= $(GIT_REPOSITORY_NAME):$(GIT_VERSION)
DOCKER_IMAGE_NAME := senzing/senzing-api-server

# Misc.

TARGET ?= target

# -----------------------------------------------------------------------------
# The first "make" target runs as default.
# -----------------------------------------------------------------------------

.PHONY: default
default: help

# -----------------------------------------------------------------------------
# Local development
# -----------------------------------------------------------------------------

.PHONY: package
package:

	mvn install:install-file \
		-Dfile=$(SENZING_G2_JAR_PATHNAME) \
		-DgroupId=com.senzing \
		-DartifactId=g2 \
		-Dversion=$(SENZING_G2_JAR_VERSION) \
		-Dpackaging=jar

	mvn package \
		-Dproject.version=$(GIT_VERSION) \
		-Dgit.branch=$(GIT_BRANCH) \
		-Dgit.repository.name=$(GIT_REPOSITORY_NAME) \
		-Dgit.sha=$(GIT_SHA) \
		-Dgit.version.long=$(GIT_VERSION_LONG)

# -----------------------------------------------------------------------------
# Docker-based package
# -----------------------------------------------------------------------------

.PHONY: docker-package
docker-package: docker-rmi-for-package
	# Make docker image.

	mkdir -p $(TARGET)
	cp $(SENZING_G2_JAR_PATHNAME) $(TARGET)/
	docker build \
		--build-arg SENZING_G2_JAR_RELATIVE_PATHNAME=$(TARGET)/g2.jar \
		--build-arg SENZING_G2_JAR_VERSION=$(SENZING_G2_JAR_VERSION) \
		--tag $(DOCKER_IMAGE_PACKAGE) \
		--file Dockerfile-package \
		.

	# Run docker image which creates a docker container.
	# Then, copy the maven output from the container to the local workstation.
	# Finally, remove the docker container.

	PID=$$(docker create $(DOCKER_IMAGE_PACKAGE) /bin/bash); \
	docker cp $$PID:/git-repository/$(TARGET) .; \
	docker rm -v $$PID

# -----------------------------------------------------------------------------
# Docker-based builds
# -----------------------------------------------------------------------------

.PHONY: docker-build
docker-build: docker-rmi-for-build
	mkdir -p $(TARGET)
	cp $(SENZING_G2_JAR_PATHNAME) $(TARGET)/
	docker build \
		--build-arg BASE_IMAGE=$(BASE_IMAGE) \
		--build-arg BASE_BUILDER_IMAGE=$(BASE_BUILDER_IMAGE) \
		--build-arg SENZING_G2_JAR_RELATIVE_PATHNAME=$(TARGET)/g2.jar \
		--build-arg SENZING_G2_JAR_VERSION=$(SENZING_G2_JAR_VERSION) \
		--tag $(DOCKER_IMAGE_NAME) \
		--tag $(DOCKER_IMAGE_NAME):$(GIT_VERSION) \
		.

.PHONY: docker-build-development-cache
docker-build-development-cache: docker-rmi-for-build-development-cache
	mkdir -p $(TARGET)
	cp $(SENZING_G2_JAR_PATHNAME) $(TARGET)/
	docker build \
		--build-arg SENZING_G2_JAR_RELATIVE_PATHNAME=$(TARGET)/g2.jar \
		--build-arg SENZING_G2_JAR_VERSION=$(SENZING_G2_JAR_VERSION) \
		--tag $(DOCKER_IMAGE_TAG) \
		.

# -----------------------------------------------------------------------------
# Clean up targets
# -----------------------------------------------------------------------------

.PHONY: docker-rmi-for-build
docker-rmi-for-build:
	-docker rmi --force \
		$(DOCKER_IMAGE_NAME):$(GIT_VERSION) \
		$(DOCKER_IMAGE_NAME)

.PHONY: docker-rmi-for-build-development-cache
docker-rmi-for-build-development-cache:
	-docker rmi --force $(DOCKER_IMAGE_TAG)

.PHONY: docker-rmi-for-package
docker-rmi-for-packagae:
	-docker rmi --force $(DOCKER_IMAGE_PACKAGE)

.PHONY: rm-target
rm-target:
	-rm -rf $(TARGET)

.PHONY: clean
clean: docker-rmi-for-build docker-rmi-for-build-development-cache docker-rmi-for-package rm-target

# -----------------------------------------------------------------------------
# Help
# -----------------------------------------------------------------------------

.PHONY: help
help:
	@echo "List of make targets:"
	@$(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print $$1}}' | sort | egrep -v -e '^[^[:alnum:]]' -e '^$@$$' | xargs
