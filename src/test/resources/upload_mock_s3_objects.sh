#!/bin/bash
# Use aws cli to upload mock documents to the decs bucket
# and mock metadata to the txa bucket
aws s3 cp /tmp/src/test/resources/s3_data/. s3://trusted-bucket --endpoint-url http://s3.localhost.localstack.cloud:4566 --exclude "*.json" --recursive
aws s3 cp /tmp/src/test/resources/s3_data/. s3://untrusted-bucket --endpoint-url http://s3.localhost.localstack.cloud:4566 --exclude "*" --include "*.json" --recursive
