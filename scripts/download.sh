#!/usr/bin/env sh

set -eo pipefail

CTLSTORE_BOOTSTRAP_URL=$1
PREFIX="$(echo $CTLSTORE_BOOTSTRAP_URL | grep :// | sed -e's,^\(.*://\).*,\1,g')"
URL="$(echo $CTLSTORE_BOOTSTRAP_URL | sed -e s,$PREFIX,,g)"
BUCKET="$(echo $URL | grep / | cut -d/ -f1)"
KEY="$(echo $URL | grep / | cut -d/ -f2)"
CONCURRENCY=${2:-20}
DOWNLOADED="false"
COMPRESSED="false"
METRICS="/var/spool/ctlstore/metrics.json"

START=$(date +%s)
END=$(date +%s)

get_head_object() {
 head_object=$(aws s3api head-object --bucket "${BUCKET}" --key "${KEY}")
 echo "$head_object"
}

if [ ! -f /var/spool/ctlstore/ldb.db ]; then
  # busybox does not support sub-second resolution
  START=$(date +%s)

  mkdir -p /var/spool/ctlstore
  cd /var/spool/ctlstore

  echo "Downloading head object from ${CTLSTORE_BOOTSTRAP_URL}"
  head_object=$(get_head_object)

  remote_checksum=$(printf '%s\n' "$head_object" | jq -r '.Metadata.checksum // empty')
  echo "Remote checksum: $remote_checksum"

  remote_version=$(printf '%s\n' "$head_object" | jq -r '.VersionId // empty')
  echo "Remote version: $remote_version"

  echo "Downloading snapshot from ${CTLSTORE_BOOTSTRAP_URL} with VersionID: ${remote_version}"
  s5cmd -r 0 --log debug cp --version-id $remote_version --concurrency $CONCURRENCY $CTLSTORE_BOOTSTRAP_URL .

  DOWNLOADED="true"
  if [[ ${CTLSTORE_BOOTSTRAP_URL: -2} == gz ]]; then
    echo "Decompressing"
    pigz -d snapshot.db.gz
    COMPRESSED="true"
  fi

  if [ -z $remote_checksum ]; then
    echo "Remote checksum is null, skipping checksum validation"
  else
    local_checksum=$(shasum -a 256 snapshot.db | cut -f1 -d\ | xxd -r -p | base64)
    echo "Local snapshot checksum: $local_checksum"

    if [[ "$local_checksum" != "$remote_checksum" ]]; then
      echo "Checksum matches"
    else
      echo "Checksum does not match"
      echo "Failed to download intact snapshot"
      exit 1
    fi
  fi

  mv snapshot.db ldb.db
  END=$(date +%s)
  echo "ldb.db ready in $(($END - $START)) seconds"
else
  echo "Snapshot already present"
fi

echo "{\"startTime\": $(($END - $START)), \"downloaded\": \"$DOWNLOADED\", \"compressed\": \"$COMPRESSED\"}" > $METRICS
cat $METRICS
