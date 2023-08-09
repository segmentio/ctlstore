#!/usr/bin/env sh

set -eo pipefail

CTLSTORE_BOOTSTRAP_URL=$1
CONCURRENCY=${2:-20}

if [ ! -f /var/spool/ctlstore/ldb.db ]; then
  mkdir -p /var/spool/ctlstore
  cd /var/spool/ctlstore
  s5cmd -r 0 --log debug cp --concurrency $CONCURRENCY $CTLSTORE_BOOTSTRAP_URL .

  if [[ ${CTLSTORE_BOOTSTRAP_URL: -2} == gz ]]; then
    echo "Decompressing"
    pigz -d snapshot.db.gz
  fi

  mv snapshot.db ldb.db
  echo "ldb.db ready"
else
  echo "Snapshot already present"
fi