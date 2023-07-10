import json
import logging
import kopf
import os
import time

from pgsqlbackups.utils import get_oldest_backupid, is_backup_id, get_backupid_from_backupinfo, get_latest_backupid, get_need_s3_env
from pgsqlbackups.constants import *
from pgsqlcommons.constants import *
from pgsqlcommons.typed import InstanceConnection, InstanceConnections
from pgsqlclusters.utiles import waiting_postgresql_ready, exec_command, waiting_instance_ready, \
    connect_machine, waiting_postgresql_recovery_completed
from pgsqlclusters.timer import correct_user_password


def is_restore_ssh_mode(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> bool:
    if spec.get(RESTORE) == None:
        return False

    if spec[RESTORE].get(RESTORE_FROMSSH) != None:
        return True

    return False


def is_restore_s3_mode(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> bool:
    if spec.get(RESTORE) == None:
        return False

    if spec.get(SPEC_S3) == None:
        logger.warning("s3 related information is not set.")
        return False

    if spec[RESTORE].get(RESTORE_FROMS3) != None:
        return True

    return False


def is_restore_mode(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> bool:
    if is_restore_ssh_mode(meta, spec, patch, status, logger):
        return True

    if is_restore_s3_mode(meta, spec, patch, status, logger):
        return True

    return False


def restore_postgresql_fromssh(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    conn: InstanceConnection,
) -> None:
    path = spec[RESTORE][RESTORE_FROMSSH][RESTORE_FROMSSH_PATH]
    address = spec[RESTORE][RESTORE_FROMSSH][RESTORE_FROMSSH_ADDRESS]

    # don't free the tmpconns
    tmpconns: InstanceConnections = InstanceConnections()
    tmpconns.add(conn)

    # wait postgresql ready
    waiting_postgresql_ready(tmpconns, logger, timeout=MINUTES * 5)

    # drop from autofailover and pause start postgresql
    cmd = ["pgtools", "-d", "-p", POSTGRESQL_PAUSE]
    exec_command(conn, cmd, logger, interrupt=True)

    waiting_instance_ready(tmpconns, logger)

    # remove old data
    cmd = ["rm", "-rf", PG_DATABASE_DIR]
    exec_command(conn, cmd, logger, interrupt=True)

    # copy data command
    if address == RESTORE_FROMSSH_LOCAL:
        ssh_conn = conn
        cmd = ["mv", path, PG_DATABASE_DIR]

        # copy data
        exec_command(conn, cmd, logger, interrupt=True)

        # change owner
        exec_command(conn, ['chown', '-R', 'postgres:postgres', DATA_DIR],
                     logger,
                     interrupt=True)
    else:
        username = address.split(":")[0]
        password = address.split(":")[1]
        host = address.split(":")[2]
        port = int(address.split(":")[3])

        cmd = [
            "sshpass", "-p", password, "scp", "-P",
            str(port), "-o", '"StrictHostKeyChecking no"', '-r',
            "%s@%s:%s %s" % (username, host, path, PG_DATABASE_DIR)
        ]
        ssh_conn = connect_machine(address)
        # copy data
        exec_command(conn, cmd, logger, interrupt=True, user='postgres')

    # remove recovery file
    #cmd =["rm", "-rf", os.path.join(PG_DATABASE_DIR, RECOVERY_CONF_FILE)]
    cmd = [
        "truncate", "--size", "0",
        os.path.join(PG_DATABASE_DIR, RECOVERY_CONF_FILE)
    ]
    exec_command(conn, cmd, logger, interrupt=True, user='postgres')

    #cmd =["rm", "-rf", os.path.join(PG_DATABASE_DIR, RECOVERY_SET_FILE)]
    cmd = [
        "truncate", "--size", "0",
        os.path.join(PG_DATABASE_DIR, RECOVERY_SET_FILE)
    ]
    exec_command(conn, cmd, logger, interrupt=True, user='postgres')

    cmd = ["rm", "-rf", os.path.join(PG_DATABASE_DIR, STANDBY_SIGNAL)]
    exec_command(conn, cmd, logger, interrupt=True)

    # remove old status data
    cmd = [
        "rm", "-rf",
        "/var/lib/postgresql/data/auto_failover/pg_autoctl/var/lib/postgresql/data/pg_data/pg_autoctl.init",
        "/var/lib/postgresql/data/auto_failover/pg_autoctl/var/lib/postgresql/data/pg_data/pg_autoctl.state"
    ]
    exec_command(conn, cmd, logger, interrupt=True)

    # resume postgresql
    cmd = ["pgtools", "-p", POSTGRESQL_RESUME]
    exec_command(conn, cmd, logger, interrupt=True)

    # waiting posgresql ready
    waiting_postgresql_ready(tmpconns, logger)

    # update password
    correct_user_password(meta, spec, patch, status, logger, conn)

    if address == RESTORE_FROMSSH_LOCAL:
        pass
    else:
        ssh_conn.free_conn()


def restore_postgresql_froms3(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    conn: InstanceConnection,
) -> None:

    logger.info("restore_postgresql_froms3")
    recovery_backupid = None
    recovery_time = None

    recovery = spec[RESTORE][RESTORE_FROMS3].get(RESTORE_FROMS3_RECOVERY, None)
    name = spec[RESTORE][RESTORE_FROMS3].get(RESTORE_FROMS3_NAME, None)

    # add s3 env by pgtools -e
    s3_info = get_need_s3_env(meta, spec, patch, status, logger,
                              [SPEC_S3, RESTORE_NAME])

    tmpconns: InstanceConnections = InstanceConnections()
    tmpconns.add(conn)

    # wait postgresql ready
    waiting_postgresql_ready(tmpconns, logger)

    # get backup info
    cmd = ["pgtools", "-v"] + s3_info
    output = exec_command(conn, cmd, logger, interrupt=True)
    if output == "":
        logger.error(f"get backup info failed, exit backup")
        raise kopf.PermanentError("get backup info failed.")
    logger.warning(f"backup verbose info = {output}")

    # process backup info
    backup_info = {BARMAN_BACKUP_LISTS: ""}
    try:
        backup_info = json.loads(output)
    except json.JSONDecodeError as e:
        logger.error(f"decode backup_info with error, {e}")

    # recovery param processing
    if recovery is None:
        pass
    elif recovery == RESTORE_FROMS3_RECOVERY_LATEST:
        recovery_time = time.strftime(DEFAULT_TIME_FORMAT, time.localtime())
    elif recovery == RESTORE_FROMS3_RECOVERY_LATEST_FULL:
        recovery_backupid = get_latest_backupid(backup_info)
    elif recovery == RESTORE_FROMS3_RECOVERY_OLDEST_FULL:
        recovery_backupid = get_oldest_backupid(backup_info)
    elif is_backup_id(recovery):
        recovery_backupid = recovery
    else:
        recovery_time = recovery

    # get backupid
    if recovery_backupid is not None:
        backupid = recovery_backupid
    elif recovery_time is not None:
        backupid = get_backupid_from_backupinfo(recovery_time, backup_info)
    else:
        backupid = None

    if backupid is None:
        logger.error(f"backupid field not valid.")
        raise kopf.PermanentError("backupid field not valid.")

    logger.warning(
        f"restore_postgresql_froms3 get backupid = {backupid}, recovery_time = {recovery_time}"
    )

    # set recovery_target_time to latest restore wal log to the end
    if recovery == RESTORE_FROMS3_RECOVERY_LATEST:
        recovery_time = RESTORE_FROMS3_RECOVERY_LATEST

    # drop from autofailover and pause start postgresql
    cmd = ["pgtools", "-d", "-p", POSTGRESQL_PAUSE]
    exec_command(conn, cmd, logger, interrupt=True)

    waiting_instance_ready(tmpconns, logger)

    # restore param
    param = ["-e", "BACKUP_ID='" + backupid + "'"]
    if recovery_time is not None:
        param.append("-e")
        param.append("RECOVERY_TIME='" + recovery_time + "'")

    cmd = ["pgtools", "-E"] + param + s3_info
    logging.warning(f"restore_postgresql_froms3 execute {cmd} to restore data")
    output = exec_command(conn, cmd, logger, interrupt=True, user="postgres")
    if output.find(SUCCESS) == -1:
        logger.error(f"execute {cmd} failed. {output}")
        raise kopf.PermanentError("restore cluster from s3 failed.")

    # remove standby.signal file
    cmd = [
        "rm", "-rf",
        os.path.join(PG_DATABASE_RESTORING_DIR, STANDBY_SIGNAL)
    ]
    exec_command(conn, cmd, logger, interrupt=True)

    # remove old status data
    cmd = [
        "rm", "-rf",
        "/var/lib/postgresql/data/auto_failover/pg_autoctl/var/lib/postgresql/data/pg_data/pg_autoctl.init",
        "/var/lib/postgresql/data/auto_failover/pg_autoctl/var/lib/postgresql/data/pg_data/pg_autoctl.state"
    ]
    exec_command(conn, cmd, logger, interrupt=True)

    ################## point-in-time recovery(pitr) recovery start

    # start postgresql by pg_ctl
    cmd = [
        "pg_ctl", "start", "-D", PG_DATABASE_RESTORING_DIR, "-l",
        os.path.join(PG_DATABASE_RESTORING_DIR, PG_LOG_FILENAME)
    ]
    output = exec_command(conn, cmd, logger, interrupt=True, user="postgres")
    logging.warning(
        f"point-in-time recovery start postgresql by pg_ctl. {output}")

    # waiting postgresql ready
    waiting_postgresql_ready(tmpconns, logger)

    # waiting recovery completed
    waiting_postgresql_recovery_completed(tmpconns, logger, timeout=HOURS)

    # sleep a lettle to wait point-in-time recovery(pitr) complete
    time.sleep(MINUTES)

    # stop postgresql by pg_ctl
    cmd = ["pg_ctl", "stop", "-D", PG_DATABASE_RESTORING_DIR]
    output = exec_command(conn, cmd, logger, interrupt=True, user="postgres")
    logging.warning(
        f"point-in-time recovery stop postgresql by pg_ctl: {output}")

    # rm recovery.signal file
    cmd = [
        "rm", "-rf",
        os.path.join(PG_DATABASE_RESTORING_DIR, RECOVERY_SIGNAL)
    ]
    exec_command(conn, cmd, logger, interrupt=True)

    # mv PG_DATABASE_RESTORING_DIR to PG_DATABASE_DIR
    cmd = ["mv", PG_DATABASE_RESTORING_DIR, PG_DATABASE_DIR]
    exec_command(conn, cmd, logger, interrupt=True)

    # chown
    cmd = ["chown", "-R", "postgres:postgres", PG_DATABASE_DIR]
    exec_command(conn, cmd, logger, interrupt=True)

    # clear recovery param
    cmd = [
        "truncate", "--size", "0",
        os.path.join(PG_DATABASE_DIR, POSTGRESQL_BACKUP_RESTORE_CONFIG)
    ]
    exec_command(conn, cmd, logger, interrupt=True, user='postgres')

    # clear recovery_finish file
    cmd = ["rm", "-rf", os.path.join(ASSIST_DIR, RECOVERY_FINISH)]
    exec_command(conn, cmd, logger, interrupt=True)

    ################## point-in-time recovery(pitr) recovery end

    time.sleep(SECONDS * 10)

    # resume postgresql
    cmd = ["pgtools", "-p", POSTGRESQL_RESUME]
    exec_command(conn, cmd, logger, interrupt=True)

    # waiting posgresql ready
    waiting_postgresql_ready(tmpconns, logger)

    # update pgautofailover_replicator password
    correct_user_password(meta, spec, patch, status, logger, conn)

    tmpconns.free_conns()


def restore_postgresql(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    conn: InstanceConnection,
) -> None:
    if is_restore_ssh_mode(meta, spec, patch, status, logger):
        restore_postgresql_fromssh(meta, spec, patch, status, logger, conn)
    if is_restore_s3_mode(meta, spec, patch, status, logger):
        restore_postgresql_froms3(meta, spec, patch, status, logger, conn)
