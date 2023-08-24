#!/usr/bin/env sh

set -eo pipefail

CTLSTORE_BOOTSTRAP_URL=$1
CONCURRENCY=${2:-20}
NUM_LDB=${3:-1}

START=$(date +%s)
END=$(date +%s)

mkdir -p /var/spool/ctlstore
cd /var/spool/ctlstore

if [ ! -f /var/spool/ctlstore/ldb.db ]; then
  # busybox does not support sub-second resolution
  START=$(date +%s)
  s5cmd -r 0 --log debug cp --concurrency $CONCURRENCY $CTLSTORE_BOOTSTRAP_URL .

  if [[ ${CTLSTORE_BOOTSTRAP_URL: -2} == gz ]]; then
    echo "Decompressing"
    pigz -d snapshot.db.gz
  fi

  i=2
  while [ "$i" -le $NUM_LDB ]; do
    if [ ! -f  ldb-$i.db ]; then
      echo "creating copy ldb-$i.db"
      cp snapshot.db ldb-$i.db
    fi
    i=$(( i + 1 ))
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
  if [ ! -f  ldb-$i.db ]; then

    # download the snapshot if it's not present
    if [ ! -f /var/spool/ctlstore/snapshot.db ]; then
      echo "Downloading a new snapshot for ldb copies"
      s5cmd -r 0 --log debug cp --concurrency $CONCURRENCY $CTLSTORE_BOOTSTRAP_URL .
    fi

    echo "creating copy ldb-$i.db"
    cp snapshot.db ldb-$i.db
  fi
  i=$(( i + 1 ))
done

if [ -f /var/spool/ctlstore/snapshot.db ]; then
  echo "removing snapshot.db"
  rm snapshot.db
fi