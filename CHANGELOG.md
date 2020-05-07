# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
[markdownlint](https://dlaa.me/markdownlint/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.8.4] - 2020-05-07

### Fixed in 1.8.4

- Updated EntityGraphServicesTest to account for bug fix in Senzing 1.15.2
- Updated test runs to include additional product versions

## [1.8.3] - 2020-04-24

### Fixed in 1.8.3

- .dockterignore was causing the `-dirty` suffix to be added to docker build versions.

## [1.8.2] - 2020-04-15

### Changed in 1.8.2

- Added WHY operations
  - GET /data-sources/{dataSourceCode}/records/{recordId}/entity/why
  - GET /entities/{entityId}/why
  - GET /why/records
- Added support for the "withFeatureStatistics" and "withDerivedFeatures"
  parameters across the following endpoints:
  - GET /data-sources/{dataSourceCode}/records/{recordId}/entity
  - GET /data-sources/{dataSourceCode}/records/{recordId}/entity/why
  - GET /entities/{entityId
  - GET /entities/{entityId}/why
  - GET /why/records
  - GET /entity-paths
  - GET /entity-networks
- Added the "featureDetails" property to entity results to support obtaining
  the feature ID as well as the feature statistics (if requested).

## [1.8.1] - 2020-03-30

### Changed in 1.8.1

- Supports environment variables for Senzing install locations
  - SENZING_G2_DIR
  - SENZING_DATA_DIR
  - SENZING_ETC_DIR
- Supports default to `/opt/senzing/data` as the support directory if the
  versioned sub-directory is not found and the base directory contains expected
  files.
- Shortens the test time for TemporaryDataCacheTest

## [1.8.0] - 2020-03-27

### Changed in 1.8.0

- Now supports and requires OpenJDK 11.0.x (Java 8 no longer supported)
- Now requires Apache Maven 3.6.1 or later
- Adds config modification functions (data sources, entity types, etc...)
- Adds `-enableAdmin` command-line option to enable adding data sources and
  entity types (if not provided then `403 Forbidden` responses)
- Adds bulk data analyze and load functions
- Adds new testing cache mechanism to replay native API calls without using the
  native API and even run auto tests without having it installed.
- Includes cached native API results for building with v1.13.4 through v1.13.8
  as well as v1.14.1 through v1.14.7

## [1.7.10] - 2020-01-29

### Changed in 1.7.10

- Update to senzing/senzing-base:1.4.0

## [1.7.9] - 2019-11-13

### Changes in 1.7.9

- Added support for MS SQL in Dockerfile by upgrading to senzing/senzing-base:1.3.0
- Updated jackson-databind to version 2.9.10.1

## [1.7.8] - 2019-10-14

### Changes in 1.7.8

- Changes to support RPM file layout on Linux for auto tests including support.
- Added JSON pretty-printing option to JsonUtils' toJsonText() functions
- Removed Dockerfile-package

## [1.7.7] - 2019-09-30

### Changes in 1.7.7

- Fixed auto tests to skip instead of fail if Senzing native libraries are not
available.
- Fixed output when command line options do not provide initialization
parameters

## [1.7.6] - 2019-09-25

### Changes in 1.7.6

- Updated dependency on FasterBind's Jackson library to version 2.9.10 to
address security vulnerabilities.
- Added missing G2ConfigMgr.destroy() call in SzApiServer during shutdown
- Updated repository manager code used for JUnit tests to check for errors
when initializing the configuration manager and when creating the standard
configuration.
- Changes to Unit Tests:
  - Updated unit tests to preserve repos if any tests associated with that
    repo failed.
  - Updated location of unit test entity repos to live in the
    `./target/test-repos` directory during a Maven build and modified the
    repo directory names to be based off the associated unit test name.
  - Updated the module name used for Senzing initialization in auto tests to
    match the current auto test for post-failure diagnostic purposes.
  - Added forced preservation of unit test entity repos passing the
    `-Dsenzing.preserve.test.repos=true` option to Maven.

## [1.7.5] - 2019-09-17

### Changes in 1.7.5

- Corrected errant definition of SzVersionInfo's `configCompatabilityVersion`
field as an integer to make it a string to allow for semantic versioning.  This
changes the response to the `GET /version` endpoint.
*NOTE*: This change may require that previously generated client stubs be
regenerated to avoid trying to parse the semantic version string as an integer.

## [1.7.4] - 2019-09-13

### Changes in 1.7.4

- Less repetitive/verbose output when invalid command line options are provided.
- Fixes auto tests when building with 1.11.1.x versions of native Senzing libs.

## [1.7.3] - 2019-08-30

### Changes in 1.7.3

- Fixed bug where the initialization of the configuration manager
(`G2ConfigMgr.initV2()`) was not checked for success or failure.  Now the
API server ensures that initialization succeeded before proceeding further.
- Removed warnings that could occur if building with version 1.11.x of g2.jar

## [1.7.2] - 2019-08-19

### Added in 1.7.2

- Added `--configmgr` option to handle managing configurations when initializing
with JSON and leveraging the configuration from the database.  This includes the
ability to migrate an INI file to JSON and conditionally upload the referenced
configuration file to the database and make it the default (see: `--migrateIni`)
- Added the ability to auto reinitialize the configuration with the latest
default configuration for the repository if the default configuration changes
while the API server is running.  This is monitored for changes every 10 seconds
and will be checked on demand if specific G2Engine functions fail.
- Added the `-configId` option to lock the API server to a specific
configuration.  When this option is used then the auto reinitialization does not
occur since the chosen configuration is likely not be the default configuration.
- Added `-readOnly` command-line option to cause the `PUT` and `POST` endpoints
for loading records to always return an `HTTP 403 Forbidden` response.
- Now a non-root, immutable container.
- RPM based installation.

### Changed in 1.7.2

- senzing/senzing-api-server:1.7.2 pinned to senzing/senzing-base:1.2.1
- Modified `SzResolvedEntity` so the `relationshipData` is populated from the
features and added auto tests to verify.  Note: this will not be provided if
`featureMode` is set to `NONE`.
- Modified `SzResolvedEntity` so the `otherData` is populated from the records
and added auto tests to verify.  Note: this will not be populated if
`forceMinimal` is true since no records will be retrieved.
- Internally upgraded the processing of command line arguments to reuse the same
functions for the ConfigurationManager and SzApiServer classes as well as the
intenral RepositoryManager class (used in auto tests).
- Fixed Junit auto tests with windows (EntityGraphServicesTest) -- worked around
libpostal bug in native Senzing API (Windows version).

### Deprecated in 1.7.2

- Deprecated `-iniFile` option in favor of newly added `-initFile`,
`-initEnvVar` and `-initJson` options to initialize with JSON instead of an INI
file.

## [1.7.1] - 2019-08-07

### Changed in 1.7.1

- Modified Makefile to disable Junit tests during Docker build until failures
specific to the Docker build can be diagnosed.

### Security in 1.7.1

- Upgraded third-party dependencies to get security patches

## [1.7.0] - 2019-07-22

### Added in 1.7.0

- Added `GET /version` endpoint to get detailed version information
- Added `GET /config/current` to get the raw configuration JSON that is
currently being used by the API Server
- Added `GET /config/default` to get a default bootstrap configuration JSON
file that can be used to compare versus the current configuration.  NOTE: this
is NOT the same as the "default configuration" in the G2ConfigMgr API, but
rather represents a brand new out-of-the-box configuration.
- Added Junit Jupiter auto tests to verify all functionality and fixed entity
path "including source" functions which were broken (as exposed by tests).
- Now requires g2.jar version 1.10.x or higher

## [1.6.1] - 2019-06-10

### Added in 1.6.1

- Merge pull request #52 from Senzing/issue-51.caceres.record-resolutio…
- …n-fields
- Added SzMatchedRecord which extends SzEntityRecord to resolve issue 51
- Added `SzMatchedRecord`, extending `SzEntityRecord`, to add match score
information to matched records in an `SzResolvedEntity`.  Modified
`SzResolvedEntity` to use `SzMatchedRecord` instead of `SzEntityRecord` for its
record list.
