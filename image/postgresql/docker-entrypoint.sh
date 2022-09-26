#!/bin/bash --login
set -Eeo pipefail

docker_setup_db() {
	local dbAlreadyExists
	dbAlreadyExists="$(psql -p $run_port --username=$POSTGRES_USER --dbname postgres --set db="$POSTGRES_DB" --tuples-only <<-'EOSQL'
			SELECT 1 FROM pg_database WHERE datname = :'db' ;
		EOSQL
	)"
	if [ -z "$dbAlreadyExists" ]; then
		psql -p $run_port --dbname postgres --username=$POSTGRES_USER -c "CREATE DATABASE $POSTGRES_DB"
	fi
}

function echo_log() {
	echo `date`"$@"
}

docker_temp_start() {
	echo_log "start postgresql"
	pg_ctl start -D $PGDATA
}

docker_temp_stop() {
	echo_log "stop postgresql"
	pg_ctl stop -D $PGDATA
}

temp_start_auto_failover() {
	cmd="$1"
	timeout="$2"
	echo_log "temp start auto failover"

	# autofailover create postgres data
	$cmd &

	# wait pg_autoctl.cfg create.
	sleep 5

	# wait postgres start
	pgtools -q "select 1" -w $timeout

	# wait autofailover init
	sleep 5
}

temp_stop_auto_failover() {
	echo_log "temp stop auto failover"

	# wait postgres flush data
	sleep 5

	# shutdown
	pg_autoctl stop --pgdata "$PGDATA" --immediate

	# clean
	rm -rf /tmp/pg_autoctl
}

function check_error() {
    ret_code="$1"
    msg="$2"
    if [ "$ret_code" != 0 ]; then
        echo `date`"$msg"
        exit 1
    fi
}

init_over() {
	echo_log "init over"
	init_file=init_finish
	echo "init postgresql finish" > "${ASSIST}/${init_file}"
}

main() {
	# data directory
	DATA=${DATA:-/var/lib/postgresql/data}
	PGDATA=${DATA}/pg_data
	export PGDATA

	if [ "$(id -u)" = '0' ]; then
		chmod 777 /tmp
		mkdir -p /var/run/postgresql/
		chmod 777 /var/run/postgresql/
		mkdir -p "$DATA"
		mkdir -p "$DATA/barman/data" "$DATA/barman/config"
		chmod 700 "$DATA"
		chown -R postgres:postgres "$DATA"
		# postgresql data directory
		mkdir -p "$PGDATA"
		chmod 700 "$PGDATA"
		chown -R postgres:postgres "$PGDATA"
	fi

	export POSTGRES_INITDB_ARGS=${POSTGRES_INITDB_ARGS:-}
	export POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-postgres}
	export POSTGRES_USER=${POSTGRES_USER:-postgres}
	export POSTGRES_DB=${POSTGRES_DB:-$POSTGRES_USER}

	if [ "$1" = 'postgres' ] && [ "$#" = 1 ] ; then
		if [ "$(id -u)" = '0' ]; then
			#exec su-exec postgres "$BASH_SOURCE" "$@" #alpine
			exec gosu postgres "$BASH_SOURCE" "$@"
		fi

		run_port=$PG_CONFIG_port
		if [ -z $run_port ]; then
			run_port=5432
		fi
		if [ -s "$PGDATA/PG_VERSION" ]; then
			run_port=$(cat ${PGDATA}/postgresql.conf | grep -w port | grep '[0-9]' | tail -n 1 | cut -d '=' -f 2 | cut -d "#" -f 1 | tr -d " ")
		fi
		export run_port

		# look specifically for PG_VERSION, as it is expected in the DB dir
		if [ -s "$PGDATA/PG_VERSION" ]; then
			echo_log 'PostgreSQL Database directory appears to contain a database; Skipping initialization'
		else
			eval 'initdb --username="$POSTGRES_USER" --pwfile=<(echo "$POSTGRES_PASSWORD") '"$POSTGRES_INITDB_ARGS"
			docker_temp_start
			docker_setup_db
			docker_temp_stop
			echo_log 'PostgreSQL init process complete; ready for start up.'
		fi
		pgtools -c -H -e PG_CONFIG_listen_addresses="'*'" -n
		exec $@
	elif [ "$1" = 'auto_failover' ] && [ "$#" = 1 ] ; then
		if [ "$(id -u)" = '0' ]; then
			#exec su-exec postgres "$BASH_SOURCE" "$@" #alpine
			exec gosu postgres "$BASH_SOURCE" "$@"
		fi

		mkdir -p ${ASSIST}
		# auto_failover data directory
		#export XDG_CONFIG_HOME=${DATA}/auto_failover
		#export XDG_DATA_HOME=${DATA}/auto_failover
		export PGPASSWORD=$AUTOCTL_REPLICATOR_PASSWORD

		monitor_port=55555
		formation=primary

		if [ "$PG_MODE" = monitor ]; then
			cmd="pg_autoctl create monitor --pgctl $PGHOME/bin/pg_ctl --pgdata $PGDATA --pgport $monitor_port --skip-pg-hba --run  --no-ssl --hostname $EXTERNAL_HOSTNAME"
			if [ ! -s "$PGDATA/PG_VERSION" ]; then
				temp_start_auto_failover "$cmd" 300

				# create formation
				pg_autoctl create formation --pgdata "$PGDATA"  --formation "$formation" --kind pgsql --enable-secondary
				check_error $? "create formation failed"

				pgtools -q "alter user autoctl_node password '$AUTOCTL_NODE_PASSWORD'"
				check_error $? "set autoctl_node password failed"

				temp_stop_auto_failover

				pgtools -c -H -n
			fi
		elif [ "$PG_MODE" = readwrite -o "$PG_MODE" = readonly ]; then
			# change port order. 1. drop node(auto_failover will exit) 2. set port 3. run and regist auto_failover
			# so we set port before start
			run_port=$PG_CONFIG_port
			if [ -z $run_port ]; then
				run_port=5432
			fi
			if [ -s "$PGDATA/PG_VERSION" ]; then
				run_port=$(cat ${PGDATA}/postgresql.conf | grep -w port | grep '[0-9]' | tail -n 1 | cut -d '=' -f 2 | cut -d "#" -f 1 | tr -d " ")
				if [ -s "$XDG_CONFIG_HOME/pg_autoctl/var/lib/postgresql/data/pg_data/pg_autoctl.cfg" ]; then
					pg_autoctl config set --pgdata "$PGDATA" postgresql.port $run_port
				fi
				#pg_autoctl config set --pgdata "$PGDATA" replication.password "$AUTOCTL_REPLICATOR_PASSWORD"
			fi
			export run_port

			cmd="pg_autoctl create postgres --pgctl $PGHOME/bin/pg_ctl --pgdata $PGDATA --pgport $run_port --hostname $EXTERNAL_HOSTNAME --username $POSTGRES_USER --dbname $POSTGRES_DB --formation $formation --monitor postgres://autoctl_node:$AUTOCTL_NODE_PASSWORD@$MONITOR_HOSTNAME:$monitor_port/pg_auto_failover?sslmode=prefer  --skip-pg-hba --no-ssl --run --maximum-backup-rate 1024M"
			if [ "$PG_MODE" = readonly ]; then
				cmd="$cmd --candidate-priority 0"
				if [ "$PG_STREAMING" = sync ]; then
					cmd="$cmd --replication-quorum 1"
				else
					cmd="$cmd --replication-quorum 0"
				fi
			fi

			# pg_autoctl config set replication.password
			# alter user pgautofailover_replicator password 'h4ckm3m0r3';
			if [ ! -s "$PGDATA/PG_VERSION" ]; then
				primary_information_num=$(psql -p $run_port -t -A -d "postgres://autoctl_node:$AUTOCTL_NODE_PASSWORD@$MONITOR_HOSTNAME:$monitor_port/pg_auto_failover?sslmode=prefer" -c "select count(*) from pgautofailover.node where formationid='primary' ")
				if [ "$PG_MODE" = readwrite -a "$primary_information_num" = 0 ]; then
					temp_start_auto_failover "$cmd" 300

					#TODO wait for pgautofailover_replicator created
					pgtools -q "alter user pgautofailover_replicator password '$AUTOCTL_REPLICATOR_PASSWORD'"
					check_error $? "set pgautofailover_replicator password failed"

					temp_stop_auto_failover

					pgtools -c -H -n
				fi
			fi
		fi

		if [ -f "$ASSIST/actual_drop" ]; then
			check_error 1 "this node is droped"
		fi

		if [ -f "$ASSIST/stop" ]; then
			echo "this node is stop, sleep 30 seconds for protect k8s/docker start it"
			sleep 30
			/bin/rm -rf "$ASSIST/stop"
		fi

		while [ -e "${ASSIST}/pause" ]
		do
			echo "start is pause, waiting"
			sleep 5
		done

		if [ -s "$PGDATA/postgresql-auto-failover-standby.conf" ]; then
			echo "delay start(10 seconds) on slave node"
			sleep 10
		fi

		# delete old pid
		rm -rf /tmp/pg_autoctl

		init_over
		exec $cmd
	else
		exec $@
	fi
}

main "$@"
