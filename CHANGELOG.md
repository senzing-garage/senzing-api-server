# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
[markdownlint](https://dlaa.me/markdownlint/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.3.0] - 2020-11-18

### Changed in 2.3.0

- Added `WhyServices.whyEntities()` method for `GET /why/entities`
- Modified `com.senzing.api.model.SzMatchInfo` to support `whyEntities()`
- Modified `com.senzing.api.model.SzScoredFeature` to support `whyEntities()`
- Modified `com.senzing.api.model.SzEntityData` to exclude `relatedEntities`
  from the JSON serialization if empty or null.
- Added `com.senzing.api.model.SzWhyEntitiesResponse`
- Added `com.senzing.api.model.SzWhyEntitiesResult`
- Added `com.senzing.api.model.SzDisclosedRelation`
- Added `com.senzing.api.model.SzRelationDirection`
- Added `com.senzing.api.model.SzRelatedFeatures`
- Refactored some functionality from `EntityDataServices` to `ServicesUtil` 
- Added new tests for `WhyServices.whyEntities()`
- Pared down the number of tests ran from `WhyServicesTest` for faster runs.
- Re-ran tests for all versions of native Senzing SDK from 2.0.0 to 2.2.5
and recorded mock test data.

## [2.2.2] - 2020-11-05

### Changed in 2.2.2

- Modified `com.senzing.api.server.G2EngineRetryHandler` so that it
recognizes new functions in `com.senzing.g2.engine.G2Engine`.
- Re-ran tests for all versions of native Senzing SDK from 2.0.0 to 2.3.0
and recorded mock test data.

## [2.2.1] - 2020-10-16

### Changed in 2.2.1

- Modified `com.senzing.api.services.EntityDataServices` so that 
`POST /data-sources/{dataSourceCode}/records` call will be tolerant of the 
`RECORD_ID` specified in the JSON payload for the record.
- Updated EntityDataWriteServicesTest to handle testing POST with various 
record ID variants.
- Re-ran tests for all versions of native Senzing SDK from 2.0.0 to 2.2.1


## [2.2.0] - 2020-10-15

### Changed in 2.2.0

- Added `com.senzing.api.model.SzNameScoring` to describe name scoring details
- Added `com.senzing.api.model.SzSearchFeatureScore` for search feature scores
- Modified `com.senzing.api.model.SzBaseRelatedEntity` to remove `fullNameScore`
field since it has not been populated since switch to version 2.0.0 of native
Senzing SDK.
- Added `bestNameScore` field to `com.senzing.api.model.SzAttributeSearchResult`
to replace `fullNameScore` in the place where the name score was previously 
used with version 1.x of the native Senzing SDK (i.e.: to sort search results
based on the strength of the name match).
- Modified `com.senzing.api.model.SzAttributeSearchResult` to include the
`featureScores` field to provide feature scores without using "raw data"
- Added `nameScoringDetails` field to `com.senzing.api.model.SzFeatureScore` 
class to provide `SzNameScoring` name scoring details on why operations,
- Updated `com.senzing.api.model.SzFeatureScore` to set its `score` field to 
the most sensible score value from the `nameScoringDetails` for `"NAME"`
features since the `FULL_SCORE` field is not available for names.
- Updated to latest `senzing-rest-api-specification` specification.
- Updated version numbers to 2.2.0
- Re-ran tests for all versions of native Senzing SDK from 2.0.0 to 2.2.1

## [2.1.1] - 2020-10-06

### Changed in 2.1.1

- No longer errors when encountering the `NOT_SCORED` value for `SzScoringBucket`
- No longer errors when encountering a numeric `RECORD_ID` on bulk data load

## [2.1.0] - 2020-10-01

### Changed in 2.1.0

- Fixed defect preventing fields in `SzMatchInfo` from being properly populated
  on "why" endpoints -- now all the matching data comes back.
- Added `SzMatchLevel` enum type to enumerate and describe the various
  match levels and added a `matchLevel` field of that type to `SzMatchInfo`
  to expose the `MATCH_LEVEL_CODE` in the "why" endpoints.
- Added `lastSeenTimestamp` field to expose `LAST_SEEN_DT` for
  `SzResolvedEntity` and `SzEntityRecord`.
- Added periodic logging of `stats()` from the `G2Engine` API with automatic
  suppression if the API Server is idle or not performing tasks that involve
  entity scoring and would therefore not affect the result from `stats()` call.
- Added `-statsInterval` option to specify the time interval in milliseconds for
  stats logging with a 15-minute default and option to suppress if zero (0) is
  specified.
- Added logging of performance information and database performance at startup
- Added `-skipStartupPerf` option to prevent startup performance logging
- Modified tests to validate `lastSeenTimestamp` field.
- Updated test data to include versions 2.0.0 through 2.1.1

## [2.0.2] - 2020-08-25

### Changed in 2.0.2

- Updated ReplayNativeApiProvider to reduce file size of test data files
  to reduce memory usage when running auto-builds on github.
- Produced new auto-test mock data files for several versions of native
  API.

## [2.0.1] - 2020-07-23

### Changed in 2.0.1

- Upgraded to senzing/senzing-base:1.5.2

## [2.0.0] - 2020-07-16

### Changed in 2.0.0

- Modified to build with version 2.x of Senzing product include use of new
  entity formatting flags.

- Updated REST API Version to v2.0.0

- Renamed `com.senzing.api.model.SzFeatureInclusion` to
  `com.senzing.api.model.SzFeatureMode`

- Added `com.senzing.api.model.SzRelationshipMode` with values of `NONE`,
  `PARTIAL` and `FULL`.

- Added new model classes to support Senzing REST API 2.0

- Added `withInfo` and `withRaw` query parameters to following endpoints:
  - `POST /data-sources/{dataSourceCode}/records`
  - `PUT /data-sources/{dataSourceCode}/records/{recordId}`

- Added `DELETE /data-sources/{dataSourceCode}/records/{recordId}` endpoint
  including `withInfo` and `withRaw` query parameters.

- Added the following endpoints to reevaluate entities or specific records
  including `withInfo` and `withRaw` query parameters:
  - `POST /data-sources/{dataSourceCode}/records/{recordId}/entity/reevaluate`
  - `POST /reevaluate-entities`

- In `com.senzing.io.RecordReader` a `null` value in the `dataSourceMap` and
  `entityTypeMap` is now used as the general overriding data source or entity
  type, respectively, while the value mapped to empty-string is used to assign
  a data source or entity type (respectively) to a record that is missing a
  value for that field.

- In `com.senzing.api.model.SzResolvedEntity` the property `attributeData` was
  renamed to `characteristicData` to match the OpenAPI Specification for the
  Senzing REST API.  **NOTE**: Client code that was written to look for
  `attributeData` must be modified or will find a missing property.

- Potentially Backward-Compatibility Breaking Changes by Java class and
  API Endpoint:
  - `com.senzing.api.services.ConfigServices`
    - `GET /entity-classes`
    - `GET /entity-classes/{entityClassCode}`
    - `POST /entity-types`
    - `POST /entity-classes/{entityClassCode}/entity-types`
    - `GET /entity-classes/{entityClassCode}/entity-types/{entityTypeCode}`
      - Removed support for any entity class other than ACTOR as it was
        discovered that the underlying product does not properly support entity
        resolution when using entity classes other than ACTOR and it may not for
        some time. This will change if and when additional entity classes are
        supported.
        - *MIGRATION*: Ensure the value provided for all entity classes are
          changed to `ACTOR`.

    - `POST /entity-classes`
      - Removed this operation as it was discovered that the underlying product
        does not fully properly support entity resolution when using entity
        classes other than ACTOR and it may not for some time.  This will change
        if and when additional entity classes are supported.
        - *MIGRATION*: Remove calls to create new entity classes and instead
          leverage the default entity class `ACTOR`.

    - `GET /config/current`
      - Renamed to `GET /configs/active` since “current” is ambiguous with
        regards to the “currently active config” versus the configuration
        managers currently configured “default config”.
        - *MIGRATION*: Use the `/configs/active` path in place of
          `/config/current`.

    - `GET /config/default`
      - Renamed to `GET /configs/template` since “default” is ambiguous with the
        configuration managers “default config” setting.
        - *MIGRATION*: Use the `/configs/template` path in place of
          `/config/default`.

  - `com.senzing.api.services.EntityDataServices`
    - `GET /data-sources/{dataSourceCode}/records/{recordId}`
      - The `data` property of `SzRecordResponse` was previously of type
        `SzEntityRecord`.  However, the Open API specification for the Senzing
         REST API had always documented it as an object with a single property
         named `record` whose type was `SzEntityRecord` (an additional level of
         indirection).  In order to conform with the specification and make it
         consistent with `SzEntityResponse`, the server has been modified with
         class `SzRecordResponse` now having a `data` property with a `record`
         sub-property.
        - *MIGRATION*: Change direct references to the `data` field to instead
          reference `data.record`.
      - The `SzEntityRecord` in the response will exclude fields that are `null`
        or empty arrays.
        - *MIGRATION*: Depending on the client language, check if fields are
          missing, `null` or `undefined` before attempting to use them.

    - `GET /entities/{entityId}`
    - `GET /data-sources/{dataSourceCode}/records/{recordId}/entity`
      - The `withRelated` parameter is no longer a `boolean` value that accepts
        `true` or `false`.  It now accepts an enumerated value of
         type `com.senzing.api.model.SzRelationshipMode` with values of `NONE`,
        `PARTIAL` or `FULL`.
        - *MIGRATION*: Use `?withRelated=FULL` in place of `?withRelated=true`
          and use `?withRelated=PARTIAL` in place of `?withRelated=false`.
      - The `SzResolvedEntity` and the contained `SzEntityRecord` instances in
        the response will exclude fields that are `null` or empty arrays.
        - *MIGRATION*: Depending on the client language, check if fields are
          missing, `null` or `undefined` before attempting to use them.

    - `GET /entities`
      - Removed the `attr_[PROPERTY_NAME]` parameters and replaced with the
        multi-valued `attr` parameter so that this parameter could better be
        documented in the Open API Spec and examples provided via Swagger
        Editor.
        - *MIGRATION*: Use `?attr=NAME_FIRST:Joe` in place of
          `?attr_NAME_FIRST=Joe` or use the `attrs` parameter with a JSON value.
      - The `SzAttributeSearchResult` instances and contained `SzRelatedEntity`
        and `SzEntityRecord` instances in the response will exclude fields that
        are `null` or empty arrays.
        - *MIGRATION*: Depending on the client language, check if fields are
          missing, `null` or `undefined` before attempting to use them.
      - The `withRelationships` query parameter now defaults to `false` instead
        of `true`.
        - *MIGRATION*: Use `?withRelationships=true` if relationships are
          desired.

    - `POST /data-sources/{dataSourceCode}/records/`
    - `PUT /data-sources/{dataSourceCode}/records/{recordId}`
      - Modified to default `ENTITY_TYPE` to `GENERIC` if `ENTITY_TYPE` not
        found in record.
        - *MIGRATION*: Specify an entity type if `GENERIC` is not desired.

  - `com.senzing.api.services.EntityGraphServices`
    - `GET /entity-networks`
      - Changed the default value for `maxDegrees` parameter from 5 to 3
        - *MIGRATION*: Use `?maxDegrees=5` if the old default is desired.
      - The `SzResolvedEntity` instances and the contained `SzEntityRecord`
        instances in the response will exclude fields that are `null` or empty
        arrays.
        - *MIGRATION*: Depending on the client language, check if fields are
          missing, `null` or `undefined` before attempting to use them.
    - `GET /entity-paths`
      - The `SzResolvedEntity` instances and the contained `SzEntityRecord`
        instances in the response will exclude fields that are `null` or empty
        arrays.
        - *MIGRATION*: Depending on the client language, check if fields are
          missing, `null` or `undefined` before attempting to use them.

  - `com.senzing.api.services.BulkDataServices`
    - `POST /bulk-data/load`
      - Replaced the `dataSource_[DATA_SOURCE_CODE]` parameters with the
        multi-valued `mapDataSource` parameter so that this parameter
        could better be documented in Open API Spec and examples provided via
        Swagger Editor.
        - *MIGRATION*: Use `?mapDataSource=FOO:BAR` in place of
          `?dataSource_FOO=BAR` or use the new `mapDataSources` parameter
           instead.
      - Replaced the `entityType_[ENTITY_TYPE_CODE]` parameters with the
        multi-valued `mapEntityType` parameter so that this parameter could
        better be documented in Open API Spec and examples provided via
        Swagger Editor.
        - *MIGRATION*: Use `?mapEntityType=FOO:BAR` in place of
          `?entityType_FOO=BAR` or use the new `mapEntityTypes` parameter instead.

- Other Changes by Java class and API Endpoint:
  - `com.senzing.api.services.AdminServices`
    - `GET /license`
      - Added the previously undocumented (but always-supported) the “withRaw”
        parameter.

    - `GET /version`
      - Added the previously undocumented (but always-supported) the “withRaw”
        parameter.

    - `POST /bulk-data/load`
      - Added the single-valued `mapDataSources` parameter which accepts
        URL-encoded JSON to map the original data sources to target data
        sources.
      - Added the single-valued `mapEntityTypes` parameter which accepts
        URL-encoded JSON to map the original entity types to target entity
        types.

- Removed pre-recorded mock data from integration tests for versions prior to
  2.0.0 and added pre-recorded mock data for integratioon tests for v2.0.0.

## [1.8.6] - 2020-10-06

### Changed in 1.8.6

- No longer errors when encountering the NOT_SCORED value for SzScoringBucket
- No longer errors when encountering a numeric RECORD_ID on bulk data load

## [1.8.5] - 2020-07-08

### Changed in 1.8.5

- Works with senzing versions up to 1.15.6
- Not supported for senzing version 2.0.0 and above

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
