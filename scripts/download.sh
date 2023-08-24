#!/usr/bin/env sh

set -eo pipefail

CTLSTORE_BOOTSTRAP_URL=$1
CONCURRENCY=${2:-20}
NUM_LDB=${3:-1}

TAGS="downloaded:false"
START=$(date +%s)
END=$(date +%s)

mkdir -p /var/spool/ctlstore
cd /var/spool/ctlstore

if [ ! -f /var/spool/ctlstore/ldb.db ]; then
  # busybox does not support sub-second resolution
  START=$(date +%s)
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

i=2
while [ "$i" -le $NUM_LDB ]; do
    echo "creating copy $i"
    cp ldb.db ldb-$i.db
    i=$(( i + 1 ))
done