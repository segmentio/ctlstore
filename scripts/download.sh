#!/usr/bin/env sh

set -eo pipefail

CTLSTORE_BOOTSTRAP_URL=$1
CONCURRENCY=${2:-20}
DOWNLOADED="false"
COMPRESSED="false"
METRICS="/var/spool/ctlstore/metrics.json"
SHASUM=""

START=$(date +%s)
END=$(date +%s)
if [ ! -f /var/spool/ctlstore/ldb.db ]; then
  # busybox does not support sub-second resolution
  START=$(date +%s)

  mkdir -p /var/spool/ctlstore
  cd /var/spool/ctlstore


  PREFIX="$(echo $CTLSTORE_BOOTSTRAP_URL | grep :// | sed -e's,^\(.*://\).*,\1,g')"
  URL="$(echo $CTLSTORE_BOOTSTRAP_URL | sed -e s,$PREFIX,,g)"
  BUCKET="$(echo $URL | grep / | cut -d/ -f1)"
  KEY="$(echo $URL | grep / | cut -d/ -f2)"

  aws s3api head-object \
    --bucket "${BUCKET}" \
    --key "${KEY}"

  s5cmd -r 0 --log debug cp --concurrency $CONCURRENCY $CTLSTORE_BOOTSTRAP_URL .

  SHASUM=$(shasum -a 256 $CTLSTORE_BOOTSTRAP_URL | cut -f1 -d\ | xxd -r -p | base64)
  echo "Sha value of the downloaded file: $(($SHASUM))"

  DOWNLOADED="true"
  if [[ ${CTLSTORE_BOOTSTRAP_URL: -2} == gz ]]; then
    echo "Decompressing"
    pigz -d snapshot.db.gz
    COMPRESSED="true"
  fi

  mv snapshot.db ldb.db
  END=$(date +%s)
  echo "ldb.db ready in $(($END - $START)) seconds"
else

  CTLSTORE_BOOTSTRAP_URL="s3://segment-ctlstore-snapshots-stage/snapshot.db"

  PREFIX="$(echo $CTLSTORE_BOOTSTRAP_URL | grep :// | sed -e's,^\(.*://\).*,\1,g')"
  URL="$(echo $CTLSTORE_BOOTSTRAP_URL | sed -e s,$PREFIX,,g)"
  BUCKET="$(echo $URL | grep / | cut -d/ -f1)"
  KEY="$(echo $URL | grep / | cut -d/ -f2)"

#  SHASUM=$(shasum -a 256 ldb.db | cut -f1 -d\ | xxd -r -p | base64)
#  echo "Sha value of the downloaded file: $(($SHASUM))"

  aws s3api head-object \
    --bucket "${BUCKET}" \
    --key "${KEY}"

  echo "Snapshot already present"
fi

echo "{\"startTime\": $(($END - $START)), \"downloaded\": \"$DOWNLOADED\", \"compressed\": \"$COMPRESSED\"}" > $METRICS
cat $METRICS
