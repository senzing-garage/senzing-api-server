# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
[markdownlint](https://dlaa.me/markdownlint/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.7.1] - 2019-08-05

### Added in 1.7.1

- Pinned to senzing/senzing-base:1.2.0
- Now a non-root, immutable container.
- RPM based installation

## [1.7.0] - 2019-07-25

### Added in 1.7.0

- Adds automated tests
- Adds GET /version endpoint
- Adds GET /config/current endpoint
- Adds GET /config/default endpoint
- Fixes bug in GET /entity-paths where specifying data sources would cause an error

## [1.6.1] - 2019-06-10

### Added in 1.6.1

- Merge pull request #52 from Senzing/issue-51.caceres.record-resolutio…
- …n-fields
- Added SzMatchedRecord which extends SzEntityRecord to resolve issue 51
