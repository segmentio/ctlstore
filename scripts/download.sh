#!/usr/bin/env sh

set -eo pipefail

CTLSTORE_BOOTSTRAP_URL=$1
CONCURRENCY=${2:-20}
STATS_IP=$3
STATS_PORT=${4:-8125}

TAGS="downloaded:false"
START=$(date +%s)
END=$(date +%s)
if [ ! -f /var/spool/ctlstore/ldb.db ]; then
  # busybox does not support sub-second resolution
  START=$(date +%s)

  mkdir -p /var/spool/ctlstore
  cd /var/spool/ctlstore
  s5cmd -r 0 --log debug cp --concurrency $CONCURRENCY $CTLSTORE_BOOTSTRAP_URL .

  TAGS="downloaded:true"
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

if [ ! -z "$STATS_IP" ]; then
  METRICS="/var/spool/ctlstore/metrics"
  echo "{'startTime': $(($END - $START))}" > $METRICS
  cat METRICS
fi
