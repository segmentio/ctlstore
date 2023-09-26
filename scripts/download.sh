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

download_snapshot() {
  s5cmd -r 0 --log debug cp --concurrency $CONCURRENCY $CTLSTORE_BOOTSTRAP_URL .
}

get_remote_checksum() {
  remote_checksum=$(aws s3api head-object --bucket "${BUCKET}" --key "${KEY}" | jq -r '.Metadata.checksum // empty')
  echo "$remote_checksum"
}

if [ ! -f /var/spool/ctlstore/ldb.db ]; then
  # busybox does not support sub-second resolution
  START=$(date +%s)

  mkdir -p /var/spool/ctlstore
  cd /var/spool/ctlstore

  COUNTER=0
  while true; do
    COUNTER=$(($COUNTER+1))

    echo "Downloading head object from ${CTLSTORE_BOOTSTRAP_URL}"
    checksum_before=$(get_remote_checksum)
    echo "Remote checksum before downloading snapshot: $checksum_before"

    echo "Downloading snapshot from ${CTLSTORE_BOOTSTRAP_URL}"
    download_snapshot

    echo "Downloading head object from ${CTLSTORE_BOOTSTRAP_URL}"
    checksum_after=$(get_remote_checksum)
    echo "Remote checksum after downloading snapshot: $checksum_after"

    DOWNLOADED="true"
    if [[ ${CTLSTORE_BOOTSTRAP_URL: -2} == gz ]]; then
      echo "Decompressing"
      pigz -d snapshot.db.gz
      COMPRESSED="true"
    fi

    if [ -z $checksum_after ]; then
      echo "Checksum is null, skipping checksum validation"
      break
    fi

    local_checksum=$(shasum -a 256 snapshot.db | cut -f1 -d\ | xxd -r -p | base64)
    echo "Local snapshot checksum: $local_checksum"

    if [[ "$local_checksum" == "$checksum_before" ]] || [[ "$local_checksum" == "$checksum_after" ]]; then
#      echo "Checksum matches"
#      break
      echo "Checksum mismatch, retrying in 1 second"
      DOWNLOADED="false"
      COMPRESSED="false"
      sleep 1
    else
      echo "Checksum mismatch, retrying in 1 second"
      DOWNLOADED="false"
      COMPRESSED="false"
      sleep 1
    fi

    if [ $COUNTER -gt 5 ]; then
      echo "Failed to download intact snapshot after 5 attempts"
      exit 1
    fi
  done

  mv snapshot.db ldb.db
  END=$(date +%s)
  echo "ldb.db ready in $(($END - $START)) seconds"
else
  echo "Snapshot already present"
fi

echo "{\"startTime\": $(($END - $START)), \"downloaded\": \"$DOWNLOADED\", \"compressed\": \"$COMPRESSED\"}" > $METRICS
cat $METRICS
