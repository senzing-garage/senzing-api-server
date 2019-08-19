# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
[markdownlint](https://dlaa.me/markdownlint/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

- Pinned to senzing/senzing-base:1.2.0
- Now a non-root, immutable container.
- RPM based installation

## [1.7.2] - 2019-08-15

- Deprecated `-iniFile` option in favor of newly added `-initFile`,
`-initEnvVar` and `-initJson` options to initialize with JSON instead of an INI
file.
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
- Internally upgraded the processing of command line arguments to reuse the same
functions for the ConfigurationManager and SzApiServer classes as well as the
intenral RepositoryManager class (used in auto tests).
- Modified `SzResolvedEntity` so the `relationshipData` is populated from the
features and added auto tests to verify.  Note: this will not be provided if
`featureMode` is set to `NONE`.
- Modified `SzResolvedEntity` so the `otherData` is populated from the records
and added auto tests to verify.  Note: this will not be populated if
`forceMinimal` is true since no records will be retrieved.
- Added `-readOnly` command-line option to cause the `PUT` and `POST` endpoints
for loading records to always return an `HTTP 403 Forbidden` response.

## [1.7.1] - 2019-08-07

- Upgraded third-party dependencies to get security patches
- Modified Makefile to disable Junit tests during Docker build until failures
specific to the Docker build can be diagnosed.

## [1.7.0] - 2019-07-22

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

- Added `SzMatchedRecord`, extending `SzEntityRecord`, to add match score
information to matched records in an `SzResolvedEntity`.  Modified
`SzResolvedEntity` to use `SzMatchedRecord` instead of `SzEntityRecord` for its
record list.

### Added in 1.6.1

- Merge pull request #52 from Senzing/issue-51.caceres.record-resolutio…
- …n-fields
- Added SzMatchedRecord which extends SzEntityRecord to resolve issue 51
