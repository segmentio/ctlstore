#!/usr/bin/env sh

set -eo pipefail

CTLSTORE_BOOTSTRAP_URL=$1
PREFIX="$(echo $CTLSTORE_BOOTSTRAP_URL | grep :// | sed -e's,^\(.*://\).*,\1,g')"
URL="$(echo $CTLSTORE_BOOTSTRAP_URL | sed -e s,$PREFIX,,g)"
BUCKET="$(echo $URL | grep / | cut -d/ -f1)"
KEY="$(echo $URL | grep / | cut -d/ -f2)"
CONCURRENCY=${2:-20}

NUM_LDB=${3:-1}

mkdir -p /var/spool/ctlstore
cd /var/spool/ctlstore

DOWNLOADED="false"
COMPRESSED="false"
METRICS="/var/spool/ctlstore/metrics.json"

# busybox does not support sub-second resolution
START=$(date +%s)
END=$(date +%s)
SHA_START=$(date +%s)
SHA_END=$(date +%s)

get_head_object() {
  head_object=$(aws s3api head-object --bucket "${BUCKET}" --key "${KEY}")
  echo "$head_object"
}

#
download_snapshot() {
  echo "Downloading head object from ${CTLSTORE_BOOTSTRAP_URL}"
  head_object=$(get_head_object)

  remote_checksum=$(printf '%s\n' "$head_object" | jq -r '.Metadata.checksum // empty')
  echo "Remote checksum in sha1: $remote_checksum"

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
}

check_sha() {
  SHA_START=$(date +%s)
  if [ -z $remote_checksum ]; then
    echo "Remote checksum sha1 is null, skipping checksum validation"
  else
    local_checksum=$(shasum snapshot.db | cut -f1 -d\  | xxd -r -p | base64)
    echo "Local snapshot checksum in sha1: $local_checksum"

    if [[ "$local_checksum" == "$remote_checksum" ]]; then
      echo "Checksum matches"
    else
      echo "Checksum does not match"
      echo "Failed to download intact snapshot"
      exit 1
    fi
  fi
  SHA_END=$(date +%s)
  echo "Local checksum calculation took $(($SHA_END - $SHA_START)) seconds"
}

if [ ! -f /var/spool/ctlstore/ldb.db ]; then
  mkdir -p /var/spool/ctlstore
  cd /var/spool/ctlstore

  download_snapshot
  check_sha

  i=2
  while [ "$i" -le $NUM_LDB ]; do
    if [ ! -f ldb-$i.db ]; then
      echo "creating copy ldb-$i.db"
      cp snapshot.db ldb-$i.db
    fi
    i=$((i + 1))
  done

  mv snapshot.db ldb.db
  END=$(date +%s)
  echo "ldb.db ready in $(($END - $START)) seconds"
else
  echo "Snapshot already present"
fi

# on existing nodes, we may already have the ldb file.
# We should download a new snapshot to avoid copying an in-use ldb.db file and risking a malformed db
i=2
while [ "$i" -le $NUM_LDB ]; do

  # make sure it's not already downloaded
  if [ ! -f ldb-$i.db ]; then

    # download the snapshot if it's not present
    if [ ! -f /var/spool/ctlstore/snapshot.db ]; then
      download_snapshot
      check_sha
    fi

    echo "creating copy ldb-$i.db"
    cp snapshot.db ldb-$i.db
  fi
  i=$((i + 1))
done

if [ -f /var/spool/ctlstore/snapshot.db ]; then
  echo "removing snapshot.db"
  rm snapshot.db
fi

echo "{\"startTime\": $(($END - $START)), \"downloaded\": \"$DOWNLOADED\", \"compressed\": \"$COMPRESSED\"}" >$METRICS
cat $METRICS
