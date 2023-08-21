#!/usr/bin/env sh

set -eo pipefail

CTLSTORE_BOOTSTRAP_URL=$1
CONCURRENCY=${2:-20}

TAGS="downloaded:false"
START=$(date +%s)
END=$(date +%s)
METRICS="/var/spool/ctlstore/metrics.json"
DOWNLOADED="false"
if [ ! -f /var/spool/ctlstore/ldb.db ]; then
  # busybox does not support sub-second resolution
  START=$(date +%s)

  mkdir -p /var/spool/ctlstore
  cd /var/spool/ctlstore
  s5cmd -r 0 --log debug cp --concurrency $CONCURRENCY $CTLSTORE_BOOTSTRAP_URL .

  TAGS="downloaded:true"
  DOWNLOADED="true"
  if [[ ${CTLSTORE_BOOTSTRAP_URL: -2} == gz ]]; then
    echo "Decompressing"
    pigz -d snapshot.db.gz
    TAGS="$TAGS,compressed:true"
  fi

  TAGS="$TAGS,concurrency:$CONCURRENCY"

  mv snapshot.db ldb.db
  END=$(date +%s)
  echo "ldb.db ready in $(($END - $START)) seconds"
else
  echo "Snapshot already present"
fi

echo "{\"startTime\": $(($END - $START)), \"downloaded\": $DOWNLOADED}" > $METRICS
cat $METRICS
