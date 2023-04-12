# hocs-txa-document-extractor

[![CodeQL](https://github.com/UKHomeOffice/hocs-txa-document-extractor/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/UKHomeOffice/hocs-txa-document-extractor/actions/workflows/codeql-analysis.yml)

This application serves as an interface between the DECS system and a Text Analytics pipeline.

The purpose of this application is to identify relevant documents for
and ingest them for analysis.

## Run it locally
To run the application locally:
1. Clone the project and ensure you have the gradle dependencies available.
2. Spin up the required infrastructure & test data with a `docker compose build` and `docker compose up`.
3. Configure the necessary properties either as environment variables or in your IDE run configuration:
   1. `export AWS_ACCESS_KEY_ID=UNSET`
   2. `export AWS_SECRET_ACCESS_KEY=UNSET`
   3. `export METADATA_LAST_INGEST="2023-03-22 11:59:59"`
4. Compile the project and run the `HocsTxaDocumentExtractorApplication` class.
5. Observe the outcome of the job in your console

## Run tests
### Local tests
TBC

### Testing via CI pipeline
TBC

## Deployments
TBC

## Versioning

For versioning this project uses SemVer.

## Authors

This project is authored by the Home Office.

## License

This project is licensed under the MIT license. For details please see [License](LICENSE)
