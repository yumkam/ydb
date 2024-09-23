#!/usr/bin/env bash
set -eux
if [ ! -d tpc/h/10${decimal+_${decimal:-decimal}} ]; then
mkdir -p tpc/h/10${decimal+_${decimal:-decimal}}
fi

b=`pwd`
cd tpc/h/10${decimal+_${decimal:-decimal}}

base=https://storage.yandexcloud.net/tpc/h/s10${decimal+_${decimal:-decimal}}/parquet

source $b/download_lib.sh
source $b/download_tables.sh

