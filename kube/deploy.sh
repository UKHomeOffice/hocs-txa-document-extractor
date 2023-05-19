#!/bin/bash

set -eo pipefail

case ${KUBE_CLUSTER} in
  'acp-notprod'|'acp-prod')
    export KUBE_CERTIFICATE_AUTHORITY=https://raw.githubusercontent.com/UKHomeOffice/acp-ca/master/${KUBE_CLUSTER}.crt
    ;;
  *)
    echo "[error] KUBE_CLUSTER must be set to acp-notprod or acp-prod"
    exit 1
esac

echo "Target cluster: ${KUBE_CLUSTER}"
echo "Target namespace: ${KUBE_NAMESPACE}"

if ! kd --timeout=10m -f kube/secrets.yml; then
  echo "[error] failed to deploy secrets"
  exit 1
fi

if ! kd --timeout=10m -f kube/ingestCronJob.yml; then
  echo "[error] failed to deploy ingestCronJob"
  exit 1
fi

if ! kd --timeout=10m -f kube/deleteCronJob.yml; then
  echo "[error] failed to deploy deleteCronJob"
  exit 1
fi
