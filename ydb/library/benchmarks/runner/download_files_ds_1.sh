#!/usr/bin/env bash
set -eux
if [ ! -d tpc/ds/1${decimal+_${decimal:-decimal}} ]; then
mkdir -p tpc/ds/1${decimal+_${decimal:-decimal}}
fi

b=`pwd`
cd tpc/ds/1${decimal+_${decimal:-decimal}}

base=https://storage.yandexcloud.net/tpc/ds/s1${decimal+_${decimal:-decimal}}/parquet

source $b/download_lib.sh
source $b/download_tpcds_tables.sh

