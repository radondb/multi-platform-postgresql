#!/bin/bash
set -Eeo pipefail

download_dir=$1
if [ -z $download_dir ]
then
	download_dir="."
fi

cd $download_dir
echo "download source code to $download_dir"

# postgresql
#/bin/rm -rf qingcloud-postgresql
#git clone git@git.internal.yunify.com:RDS/qingcloud-postgresql.git
if [ -d postgresql ]
then
	echo "postgresql exists, skip download."
else
	git clone https://git.postgresql.org/git/postgresql.git
	if [ $? -ne 0 ]
	then
		echo "download postgresql failed"
		exit 1
	fi
	echo "download postgresql success"
fi

# auto_failover
#/bin/rm -rf polondb-pg_auto_failover
#git clone git@git.internal.yunify.com:RDS/polondb-pg_auto_failover.git
if [ -d pg_auto_failover ]
then
	echo "pg_auto_failover exists, skip download."
else
	git clone https://github.com/citusdata/pg_auto_failover.git
	if [ $? -ne 0 ]
	then
		echo "download auto_failover failed"
		exit 1
	fi
	echo "download pg_auto_failover success"
fi

# pg_dirtyread
if [ -d pg_dirtyread ]
then
	echo "pg_dirtyread exists, skip download."
else
	git clone https://github.com/df7cb/pg_dirtyread.git
	if [ $? -ne 0 ]
	then
		echo "download pg_dirtyread failed"
		exit 1
	fi
	echo "download pg_dirtyread success"
fi

# citus
if [ -d citus ]
then
	echo "citus exists, skip download."
else
	git clone https://github.com/citusdata/citus.git
	if [ $? -ne 0 ]
	then
		echo "download citus failed"
		exit 1
	fi
	echo "download citus success"
fi

# postgis
if [ -d postgis ]
then
	echo "postgis exists, skip download."
else
	git clone https://github.com/postgis/postgis.git
	if [ $? -ne 0 ]
	then
		echo "download postgis failed"
		exit 1
	fi
	echo "download postgis success"
fi

exit 0
