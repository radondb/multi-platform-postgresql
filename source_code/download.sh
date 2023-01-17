#!/bin/bash
set -Eeo pipefail

download_dir=$1
if [ -z $download_dir ]
then
	download_dir=$(pwd)
fi

cd $download_dir
echo "download source code to $download_dir"

function download_source_code() {
    name=$1
    url=$2
    if [ -d $name ]
    then
      echo "$name exists, skip download."
    else
      git clone $url
      if [ $? -ne 0 ]
      then
        echo "download $name failed"
        exit 1
      fi
      echo "download $name success"
    fi
}

# postgresql
#/bin/rm -rf qingcloud-postgresql
#git clone git@git.internal.yunify.com:RDS/qingcloud-postgresql.git
download_source_code "postgresql" "https://git.postgresql.org/git/postgresql.git"

# auto_failover
#/bin/rm -rf polondb-pg_auto_failover
#git clone git@git.internal.yunify.com:RDS/polondb-pg_auto_failover.git
download_source_code "pg_auto_failover" "https://github.com/citusdata/pg_auto_failover.git"

# pg_dirtyread
download_source_code "pg_dirtyread" "https://github.com/df7cb/pg_dirtyread.git"

# citus
download_source_code "citus" "https://github.com/citusdata/citus.git"

# postgis
download_source_code "postgis" "https://github.com/postgis/postgis.git"

# barman
download_source_code "barman" "https://github.com/EnterpriseDB/barman.git"

exit 0
