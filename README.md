# hocs-txa-document-extractor

[![CodeQL](https://github.com/UKHomeOffice/hocs-txa-document-extractor/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/UKHomeOffice/hocs-txa-document-extractor/actions/workflows/codeql-analysis.yml)

This application serves as an interface between the DECS system and a Text Analytics pipeline.

The purpose of this application is to identify relevant documents for text analytics
and ingest them for analysis.


## Run tests
### Unit Tests
The application unit tests do not require any external dependencies.

The unit tests (located in `src/test/`) can be executed through your IDE.

### Integration Tests
The application integration tests do require some containerised infrastructure
to be running.
1. A postgres database
2. 2 x S3 buckets
3. A Kafka cluster

This infrastructure can be spun up using the hocs-ci-infrastructure submodule
with a `docker compose -f ./ci/docker-compose.yml up localstack postgres`.

The integration tests (located in `src/integration-test/`) can then be executed
through your IDE.

The integration tests each upload some data to the postgres database, upload
some files to the s3 buckets and then run the Spring Batch job to test what
happens in different scenarios. All data and files are removed during the tear
down of each test.


## CI & Deployments
TBC

## Schema Changes
If the schema of a document (`DocumentRow` class) is going to change in future, please be
sure to let the text analytics team know so they can prepare for these changes downstream.

## Versioning

For versioning this project uses SemVer.

## Authors

This project is authored by the Home Office.

## License

This project is licensed under the MIT license. For details please see [License](LICENSE)
