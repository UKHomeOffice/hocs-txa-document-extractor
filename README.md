# hocs-txa-document-extractor

[![CodeQL](https://github.com/UKHomeOffice/hocs-txa-document-extractor/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/UKHomeOffice/hocs-txa-document-extractor/actions/workflows/codeql-analysis.yml)

This application serves as an interface between the DECS system and a Text Analytics pipeline.

The purpose of this application is to identify relevant documents for text analytics
and ingest them for analysis. It is also used to identify documents soft-deleted from
the DECS system and propagate these downstream.


## Run tests
### Unit Tests
The application unit tests do not require any external dependencies.

The unit tests (located in `src/test/`) can be executed through your IDE or with a
`./gradlew test --no-daemon`.

### Integration Tests
The application integration tests do require some containerised infrastructure
to be running.
1. A postgres database
2. 2 x S3 buckets
3. A Kafka cluster

This infrastructure can be spun up using the hocs-ci-infrastructure submodule
with a `docker compose -f ./ci/docker-compose.yml -f ./ci/docker-compose.kafka.yml up localstack postgres zookeeper broker`.

The integration tests (located in `src/integration-test/`) can then be executed
through your IDE or with a `./gradlew integrationTest --no-daemon`.

The integration tests each upload some data to the postgres database, upload
some files to the s3 buckets and then run the Spring Batch job to test what
happens in different scenarios. All data and files are removed during the tear
down of each test.

**_NOTE:_** `./ci/docker-compose.kafka.yml` also includes an optional control-center service
which you can choose to docker compose up too. This provides a UI for the Kafka cluster
on `localhost:9021` which can be used to aid manual testing & debugging.

## CI & Deployments
Tests, security & quality scans, and docker builds/pushes are run using GitHub actions and
are triggered on merge request into the default branch.
See the `.github/workflows` directory.

Deployments to notprod are implemented by the `.drone.yml` file in this repo.

Deployments to prod namespaces are managed according to the HOCS Deployments
Documentation - https://ukhomeoffice.github.io/hocs/deployments/

## Schema Changes
If the schema of a document (`DocumentRow` class) is going to change in future, please be
sure to let the text analytics team know so they can prepare for these changes downstream.

## Versioning

For versioning this project uses SemVer.

## Authors

This project is authored by the Home Office.

## License

This project is licensed under the MIT license. For details please see [License](LICENSE)
