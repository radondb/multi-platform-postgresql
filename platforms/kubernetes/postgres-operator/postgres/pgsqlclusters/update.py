import kopf
import logging
import time
import copy
import traceback
import os

from pgsqlcommons.constants import *
from pgsqlcommons.typed import LabelType, InstanceConnection, InstanceConnections, TypedDict, InstanceConnectionMachine, InstanceConnectionK8S, Tuple, Any, List
import pgsqlbackups.backup as pgsql_backup
import pgsqlbackups.utils as backup_util
import pgsqlbackups.restore as backup_restore
import pgsqlclusters.create as pgsql_create
import pgsqlclusters.delete as pgsql_delete
import pgsqlclusters.utiles as pgsql_util


def fuzzy_matching(data: Any, match_data: Tuple) -> bool:
    ldata = str(data)

    for md in match_data:
        if ldata.find(str(md)) == -1:
            return False

    return True


def trigger_backup_to_s3_manual(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    AC: str,
    FIELD: Tuple,
    OLD: Any,
    NEW: Any,
) -> bool:
    if FIELD[0:len(DIFF_FIELD_SPEC_BACKUPS3_MANUAL
                   )] == DIFF_FIELD_SPEC_BACKUPS3_MANUAL or (
                       FIELD[0:len(DIFF_FIELD_SPEC_BACKUPCLUSTER)]
                       == DIFF_FIELD_SPEC_BACKUPCLUSTER and AC == "add"
                       and OLD is None and fuzzy_matching(
                           NEW, DIFF_FIELD_SPEC_BACKUPS3_MANUAL[len(
                               DIFF_FIELD_SPEC_BACKUPCLUSTER
                           ):len(DIFF_FIELD_SPEC_BACKUPS3_MANUAL)]) == True):
        if backup_util.get_backup_mode(
                meta, spec, patch, status, logger,
            [BACKUP_MODE_S3_MANUAL]) == BACKUP_MODE_S3_MANUAL:
            pgsql_backup.backup_postgresql_by_main_cr(meta, spec, patch,
                                                      status, logger)


def trigger_rebuild_postgresql(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    AC: str,
    FIELD: Tuple,
    OLD: Any,
    NEW: Any,
):
    if FIELD[0:len(DIFF_FIELD_SPEC_REBUILD_NODENAMES
                   )] == DIFF_FIELD_SPEC_REBUILD_NODENAMES or (
                       FIELD[0:len(DIFF_FIELD_SPEC_REBUILD)]
                       == DIFF_FIELD_SPEC_REBUILD and AC == "add"
                       and OLD is None
                       and fuzzy_matching(NEW,
                                          (SPCE_REBUILD_NODENAMES, )) == True):
        rebuild_postgresqls(meta,
                            spec,
                            patch,
                            status,
                            logger,
                            delete_disk=True)


def trigger_switchover_postgresql(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    AC: str,
    FIELD: Tuple,
    OLD: Any,
    NEW: Any,
):
    if FIELD[0:len(DIFF_FIELD_SPEC_SWITCHOVER_MASTERNODE
                   )] == DIFF_FIELD_SPEC_SWITCHOVER_MASTERNODE or (
                       FIELD[0:len(DIFF_FIELD_SPEC_SWITCHOVER)]
                       == DIFF_FIELD_SPEC_SWITCHOVER and AC == "add"
                       and OLD is None and fuzzy_matching(
                           NEW, (SPEC_SWITCHOVER_MASTERNODE, )) == True):
        switchover_postgresqls(meta, spec, patch, status, logger)


def update_streaming(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    AC: str,
    FIELD: Tuple,
    OLD: Any,
    NEW: Any,
) -> bool:
    need_update_number_sync_standbys = False
    if FIELD == DIFF_FIELD_STREAMING:
        if AC != DIFF_CHANGE:
            logger.error(DIFF_FIELD_STREAMING + " only support " + DIFF_CHANGE)
        else:
            conns = pgsql_util.connections(
                spec, meta, patch,
                pgsql_util.get_field(POSTGRESQL, READONLYINSTANCE), False,
                None, logger, None, status, False)
            need_update_number_sync_standbys = set_postgresql_streaming(
                meta, spec, patch, status, logger, NEW, conns.get_conns())
            conns.free_conns()

    return need_update_number_sync_standbys


def set_postgresql_streaming(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    streaming: str,
    conns: [InstanceConnection],
    update_sync: bool = False,
    force_disaster: bool = False,
) -> bool:
    need_update_number_sync_standbys = False

    def local_update_number_sync_standbys():
        pgsql_util.waiting_cluster_final_status(meta, spec, patch, status,
                                                logger)
        pgsql_util.update_number_sync_standbys(meta,
                                               spec,
                                               patch,
                                               status,
                                               logger,
                                               force_disaster=force_disaster)

    if streaming == STREAMING_SYNC:
        quorum = 1
        need_update_number_sync_standbys = True
    elif streaming == STREAMING_ASYNC:
        quorum = 0
        # must set number before set async
        logger.info(
            "waiting for update_cluster success when treaming set to async")
        local_update_number_sync_standbys()
    cmd = ["pgtools", "-S", "'node replication-quorum " + str(quorum) + "'"]
    logger.info(f"set streaming with cmd {cmd}")
    for conn in conns:
        i = 0
        while True:
            output = pgsql_util.exec_command(conn,
                                             cmd,
                                             logger,
                                             interrupt=False)
            if output.find(SUCCESS) == -1:
                logger.error(f"set streaming failed {cmd}  {output}")
                i += 1
                if i >= 60:
                    logger.error(f"set streaming failed, skip")
                    break
            else:
                break

    if update_sync == True and need_update_number_sync_standbys == True:
        local_update_number_sync_standbys()
        need_update_number_sync_standbys = False

    return need_update_number_sync_standbys


def update_action(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    AC: str,
    FIELD: Tuple,
    OLD: Any,
    NEW: Any,
) -> None:
    if FIELD == DIFF_FIELD_ACTION:
        if AC != DIFF_CHANGE:
            #raise kopf.TemporaryError("Exception when calling list_pod_for_all_namespaces: %s\n" % e)
            logger.error(
                str(DIFF_FIELD_ACTION) + " only support " + DIFF_CHANGE)
        else:
            conns = []
            # stop autofailover first. protect readwrite switch failover.
            # start autofailover first, postgresql will connect it
            autofailover_conns = pgsql_util.connections(
                spec, meta, patch, pgsql_util.get_field(AUTOFAILOVER), False,
                None, logger, None, status, False)
            conns += autofailover_conns.get_conns()
            readwrite_conns = pgsql_util.connections(
                spec, meta, patch,
                pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE), False,
                None, logger, None, status, False)
            conns += readwrite_conns.get_conns()
            readonly_conns = pgsql_util.connections(
                spec, meta, patch,
                pgsql_util.get_field(POSTGRESQL, READONLYINSTANCE), False,
                None, logger, None, status, False)
            conns += readonly_conns.get_conns()
            if NEW == ACTION_STOP:
                start = False
            elif NEW == ACTION_START:
                start = True
            for conn in conns:
                pgsql_util.postgresql_action(meta, spec, patch, status, logger,
                                             conn, start)

            if NEW == ACTION_START:
                pgsql_util.waiting_postgresql_ready(readwrite_conns, logger)
                pgsql_util.waiting_postgresql_ready(readonly_conns, logger)
                pgsql_util.waiting_cluster_final_status(
                    meta, spec, patch, status, logger)

            autofailover_conns.free_conns()
            readwrite_conns.free_conns()
            readonly_conns.free_conns()


def get_vct_size(vct: TypedDict) -> str:
    return vct["spec"]["resources"]["requests"]["storage"]


def rolling_update(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    target_roles: List,
    exit: bool = False,
    delete_disk: bool = False,
    timeout: int = MINUTES * 5,
) -> None:
    if target_roles is None:
        return

    # rolling update autofailover, not allow autofailover delete disk when update cluster
    field = pgsql_util.get_field(AUTOFAILOVER)
    if field in target_roles:
        autofailover_machines = spec.get(AUTOFAILOVER).get(MACHINES)
        if autofailover_machines != None:
            pgsql_delete.delete_autofailover(meta, spec, patch, status, logger,
                                             field, autofailover_machines,
                                             None, False)
        else:
            pgsql_delete.delete_autofailover(meta, spec, patch, status, logger,
                                             field, None, [0, 1], False)
            for vct in spec.get(AUTOFAILOVER).get(VOLUMECLAIMTEMPLATES):
                if vct["metadata"]["name"] == POSTGRESQL_PVC_NAME:
                    size = get_vct_size(vct)
            pgsql_util.resize_pvc(
                meta, spec, patch, status, logger,
                pgsql_util.get_pvc_name(
                    pgsql_util.get_pod_name(meta["name"], field, 0)), size)
        pgsql_create.create_autofailover(
            meta, spec, patch, status, logger,
            pgsql_util.get_autofailover_labels(meta))
        # wait postgresql ready, then wait the right status.
        pgsql_util.waiting_cluster_final_status(meta, spec, patch, status,
                                                logger)

    # rolling update readwrite
    field = pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE)
    if field in target_roles:
        readwrite_machines = spec.get(POSTGRESQL).get(READWRITEINSTANCE).get(
            MACHINES)
        if readwrite_machines != None:
            for replica in range(0, len(readwrite_machines)):
                pgsql_delete.delete_postgresql_readwrite(
                    meta, spec, patch, status, logger, field,
                    readwrite_machines[replica:replica + 1], None, delete_disk)
                pgsql_create.create_postgresql_readwrite(
                    meta, spec, patch, status, logger,
                    pgsql_util.get_readwrite_labels(meta), replica, False,
                    replica + 1)
                # wait postgresql ready, then wait the right status.
                pgsql_util.waiting_target_postgresql_ready(
                    meta, spec, patch, field, status, logger, replica,
                    replica + 1, exit, timeout)
                pgsql_util.waiting_cluster_final_status(
                    meta, spec, patch, status, logger)
        else:
            for replica in range(
                    0, spec[POSTGRESQL][READWRITEINSTANCE][REPLICAS]):
                pgsql_delete.delete_postgresql_readwrite(
                    meta, spec, patch, status, logger, field, None,
                    [replica, replica + 1], delete_disk)
                if delete_disk == False:
                    for vct in spec.get(POSTGRESQL).get(READWRITEINSTANCE).get(
                            VOLUMECLAIMTEMPLATES):
                        if vct["metadata"]["name"] == POSTGRESQL_PVC_NAME:
                            size = get_vct_size(vct)
                    pgsql_util.resize_pvc(
                        meta, spec, patch, status, logger,
                        pgsql_util.get_pvc_name(
                            pgsql_util.get_pod_name(meta["name"], field,
                                                    replica)), size)
                pgsql_create.create_postgresql_readwrite(
                    meta, spec, patch, status, logger,
                    pgsql_util.get_readwrite_labels(meta), replica, False,
                    replica + 1)
                # wait postgresql ready, then wait the right status.
                pgsql_util.waiting_target_postgresql_ready(
                    meta, spec, patch, field, status, logger, replica,
                    replica + 1, exit, timeout)
                pgsql_util.waiting_cluster_final_status(
                    meta, spec, patch, status, logger)

    # rolling update readonly
    field = pgsql_util.get_field(POSTGRESQL, READONLYINSTANCE)
    if field in target_roles:
        readonly_machines = spec.get(POSTGRESQL).get(READONLYINSTANCE).get(
            MACHINES)
        if readonly_machines != None:
            for replica in range(0, len(readonly_machines)):
                pgsql_delete.delete_postgresql_readonly(
                    meta, spec, patch, status, logger, field,
                    readonly_machines[replica:replica + 1], None, delete_disk)
                pgsql_create.create_postgresql_readonly(
                    meta, spec, patch, status, logger,
                    pgsql_util.get_readonly_labels(meta), replica, replica + 1)
                # wait postgresql ready, then wait the right status.
                pgsql_util.waiting_target_postgresql_ready(
                    meta, spec, patch, field, status, logger, replica,
                    replica + 1, exit, timeout)
                pgsql_util.waiting_cluster_final_status(
                    meta, spec, patch, status, logger)
        else:
            for replica in range(0,
                                 spec[POSTGRESQL][READONLYINSTANCE][REPLICAS]):
                pgsql_delete.delete_postgresql_readonly(
                    meta, spec, patch, status, logger, field, None,
                    [replica, replica + 1], delete_disk)
                if delete_disk == False:
                    for vct in spec.get(POSTGRESQL).get(READONLYINSTANCE).get(
                            VOLUMECLAIMTEMPLATES):
                        if vct["metadata"]["name"] == POSTGRESQL_PVC_NAME:
                            size = get_vct_size(vct)
                    pgsql_util.resize_pvc(
                        meta, spec, patch, status, logger,
                        pgsql_util.get_pvc_name(
                            pgsql_util.get_pod_name(meta["name"], field,
                                                    replica)), size)
                pgsql_create.create_postgresql_readonly(
                    meta, spec, patch, status, logger,
                    pgsql_util.get_readonly_labels(meta), replica, replica + 1)
                # wait postgresql ready, then wait the right status.
                pgsql_util.waiting_target_postgresql_ready(
                    meta, spec, patch, field, status, logger, replica,
                    replica + 1, exit, timeout)
                pgsql_util.waiting_cluster_final_status(
                    meta, spec, patch, status, logger)


def update_podspec_volume(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    AC: str,
    FIELD: Tuple,
    OLD: Any,
    NEW: Any,
) -> None:

    if FIELD[0:len(DIFF_FIELD_AUTOFAILOVER_PODSPEC
                   )] == DIFF_FIELD_AUTOFAILOVER_PODSPEC or FIELD[
                       0:len(DIFF_FIELD_AUTOFAILOVER_VOLUME
                             )] == DIFF_FIELD_AUTOFAILOVER_VOLUME:
        rolling_update(meta,
                       spec,
                       patch,
                       status,
                       logger, [pgsql_util.get_field(AUTOFAILOVER)],
                       exit=True,
                       timeout=MINUTES * 5)
    if FIELD[0:len(DIFF_FIELD_READWRITE_PODSPEC
                   )] == DIFF_FIELD_READWRITE_PODSPEC or FIELD[
                       0:len(DIFF_FIELD_READWRITE_VOLUME
                             )] == DIFF_FIELD_READWRITE_VOLUME:
        rolling_update(meta,
                       spec,
                       patch,
                       status,
                       logger,
                       [pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE)],
                       exit=True,
                       timeout=MINUTES * 5)
    if FIELD[0:len(DIFF_FIELD_READONLY_PODSPEC
                   )] == DIFF_FIELD_READONLY_PODSPEC or FIELD[
                       0:len(DIFF_FIELD_READONLY_VOLUME
                             )] == DIFF_FIELD_READONLY_VOLUME:
        rolling_update(meta,
                       spec,
                       patch,
                       status,
                       logger,
                       [pgsql_util.get_field(POSTGRESQL, READONLYINSTANCE)],
                       exit=True,
                       timeout=MINUTES * 5)


def update_antiaffinity(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    target_roles: List,
    exit: bool = False,
    delete_disk: bool = False,
    timeout: int = MINUTES * 5,
) -> None:
    # local volume
    if spec.get(SPEC_VOLUME_TYPE, 'local') == SPEC_VOLUME_LOCAL:
        delete_disk = True
        timeout = HOURS * 1
    rolling_update(meta, spec, patch, status, logger, target_roles, exit,
                   delete_disk, timeout)


def update_replicas(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    AC: str,
    FIELD: Tuple,
    OLD: Any,
    NEW: Any,
) -> bool:
    need_update_number_sync_standbys = False
    if FIELD == DIFF_FIELD_READWRITE_REPLICAS:
        if AC != DIFF_CHANGE:
            #raise kopf.TemporaryError("Exception when calling list_pod_for_all_namespaces: %s\n" % e)
            logger.error(
                str(DIFF_FIELD_ACTION) + " only support " + DIFF_CHANGE)
        else:
            if NEW > OLD:
                pgsql_create.create_postgresql_readwrite(
                    meta, spec, patch, status, logger,
                    pgsql_util.get_readwrite_labels(meta), OLD, False)
            else:
                pgsql_delete.delete_postgresql_readwrite(
                    meta, spec, patch, status, logger,
                    pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE), None,
                    [NEW, OLD], True)
            need_update_number_sync_standbys = True

    if FIELD == DIFF_FIELD_READWRITE_MACHINES:
        if AC != DIFF_CHANGE:
            #raise kopf.TemporaryError("Exception when calling list_pod_for_all_namespaces: %s\n" % e)
            logger.error(
                str(DIFF_FIELD_ACTION) + " only support " + DIFF_CHANGE)
        else:
            if len(NEW) > len(OLD):
                pgsql_create.create_postgresql_readwrite(
                    meta, spec, patch, status, logger,
                    pgsql_util.get_readwrite_labels(meta), len(OLD), False)
            else:
                pgsql_delete.delete_postgresql_readwrite(
                    meta, spec, patch, status, logger,
                    pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE),
                    [i for i in OLD if i not in NEW], None, True)
            pgsql_delete.delete_services(meta, spec, patch, status, logger)
            pgsql_create.create_services(meta, spec, patch, status, logger)

            need_update_number_sync_standbys = True

    if FIELD == DIFF_FIELD_READONLY_REPLICAS:
        if NEW > OLD:
            pgsql_create.create_postgresql_readonly(
                meta, spec, patch, status, logger,
                pgsql_util.get_readonly_labels(meta), OLD)
        else:
            pgsql_delete.delete_postgresql_readonly(
                meta, spec, patch, status, logger,
                pgsql_util.get_field(POSTGRESQL, READONLYINSTANCE), None,
                [NEW, OLD], True)
        need_update_number_sync_standbys = True

    if FIELD == DIFF_FIELD_READONLY_MACHINES:
        if OLD == None or (NEW != None and len(NEW) > len(OLD)):
            if OLD == None:
                begin = 0
            else:
                begin = len(OLD)
            pgsql_create.create_postgresql_readonly(
                meta, spec, patch, status, logger,
                pgsql_util.get_readonly_labels(meta), begin)
        else:
            if NEW == None:
                delete_machine = OLD
            else:
                delete_machine = [i for i in OLD if i not in NEW]
            pgsql_delete.delete_postgresql_readonly(
                meta, spec, patch, status, logger,
                pgsql_util.get_field(POSTGRESQL, READONLYINSTANCE),
                delete_machine, None, True)
        pgsql_delete.delete_services(meta, spec, patch, status, logger)
        pgsql_create.create_services(meta, spec, patch, status, logger)

        need_update_number_sync_standbys = True

    return need_update_number_sync_standbys


def update_service(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    AC: str,
    FIELD: Tuple,
    OLD: Any,
    NEW: Any,
) -> None:
    if FIELD == DIFF_FIELD_SERVICE:
        pgsql_delete.delete_services(meta, spec, patch, status, logger)
        pgsql_create.create_services(meta, spec, patch, status, logger)


def update_node_priority(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    conns: [InstanceConnection],
    priority: int,
    primary_host: str = None,
) -> bool:
    cmd = ["pgtools", "-S", "'node candidate-priority " + str(priority) + "'"]
    if primary_host == None:
        primary_host = pgsql_util.get_primary_host(meta, spec, patch, status,
                                                   logger)

    for conn in conns:
        # cannot set primary priority to 0, but allow set another readwrite to 0(use in adjust postgresql param)
        if pgsql_util.get_connhost(
                conn) == primary_host and priority == NODE_PRIORITY_NEVER:
            continue
        i = 0
        while True:
            logger.info(f"set node priority {cmd} on %s" %
                        pgsql_util.get_connhost(conn))
            output = pgsql_util.exec_command(conn,
                                             cmd,
                                             logger,
                                             interrupt=False)
            if output.find(SUCCESS) == -1:
                logger.error(f"set node priority failed {cmd}  {output}")
                i += 1
                if i >= 60:
                    logger.error(f"set node priority failed")
                    break
            else:
                break


def update_configs_utile(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    conns: [InstanceConnection],
    readwrite_conns: InstanceConnections,
    readonly_conns: InstanceConnections,
    cmd: TypedDict,
    autofailover: bool,
    restart: bool,
) -> None:
    primary_host = pgsql_util.get_primary_host(meta, spec, patch, status,
                                               logger)
    if restart == True:
        cmd.append('-r')
    # pg_autoctl set node candidate-priority 0 --pgdata=s

    if autofailover == False and restart == True:
        update_node_priority(meta, spec, patch, status, logger,
                             readwrite_conns.get_conns(), NODE_PRIORITY_NEVER,
                             primary_host)

    # first update primary node
    checkpoint_cmd = ["pgtools", "-w", "0", "-q", "'checkpoint'"]
    for conn in conns:
        if pgsql_util.get_connhost(conn) == primary_host:
            output = pgsql_util.exec_command(conn,
                                             checkpoint_cmd,
                                             logger,
                                             interrupt=False)
            if output.find(SUCCESS_CHECKPOINT) == -1:
                logger.error(
                    f"update configs {checkpoint_cmd} failed. {output}")
            logger.info(f"update configs {cmd} on %s" %
                        pgsql_util.get_connhost(conn))
            output = pgsql_util.exec_command(conn,
                                             cmd,
                                             logger,
                                             interrupt=False)
            if output.find(SUCCESS) == -1:
                logger.error(f"update configs {cmd} failed. {output}")

    if autofailover == False and restart == True:
        pgsql_util.waiting_postgresql_ready(readwrite_conns, logger)
        pgsql_util.waiting_cluster_final_status(meta, spec, patch, status,
                                                logger)
        # must waittin for special_change parameter send to slave
        for conn in conns:
            if pgsql_util.get_connhost(conn) == primary_host:
                output = pgsql_util.exec_command(conn,
                                                 checkpoint_cmd,
                                                 logger,
                                                 interrupt=False)
                if output.find(SUCCESS_CHECKPOINT) == -1:
                    logger.error(
                        f"update configs {checkpoint_cmd} failed. {output}")
        time.sleep(10)

    # update slave node
    for conn in conns:
        if pgsql_util.get_connhost(conn) == primary_host:
            continue

        logger.info(f"update configs {cmd} on %s" %
                    pgsql_util.get_connhost(conn))
        output = pgsql_util.exec_command(conn, cmd, logger, interrupt=False)
        if output.find(SUCCESS) == -1:
            logger.error(f"update configs {cmd} failed. {output}")

    pgsql_util.waiting_postgresql_ready(readwrite_conns, logger)
    pgsql_util.waiting_postgresql_ready(readonly_conns, logger)
    pgsql_util.waiting_cluster_final_status(meta, spec, patch, status, logger)
    if autofailover == False and restart == True:
        update_node_priority(meta, spec, patch, status, logger,
                             readwrite_conns.get_conns(),
                             NODE_PRIORITY_DEFAULT, primary_host)
    pgsql_util.waiting_cluster_final_status(meta, spec, patch, status, logger)


def update_configs_port(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    conns: [InstanceConnection],
    readwrite_conns: InstanceConnections,
    readonly_conns: InstanceConnections,
    cmd: TypedDict,
    autofailover: bool,
) -> None:
    primary_host = pgsql_util.get_primary_host(meta, spec, patch, status,
                                               logger)
    cmd.append('-d')

    # first update slave node
    for conn in conns:
        if pgsql_util.get_connhost(conn) == primary_host:
            continue

        logger.info(f"update configs {cmd} on %s" %
                    pgsql_util.get_connhost(conn))
        output = pgsql_util.exec_command(conn, cmd, logger, interrupt=False)
        if output.find(SUCCESS) == -1:
            logger.error(f"update configs {cmd} failed. {output}")

    if autofailover == False:
        pgsql_util.waiting_postgresql_ready(readwrite_conns, logger)
        pgsql_util.waiting_postgresql_ready(readonly_conns, logger)
        pgsql_util.waiting_cluster_final_status(meta, spec, patch, status,
                                                logger)

    # update primary node
    for conn in conns:
        if pgsql_util.get_connhost(conn) == primary_host:
            if autofailover == False and len(readwrite_conns.get_conns()) > 1:
                pgsql_util.autofailover_switchover(
                    meta,
                    spec,
                    patch,
                    status,
                    logger,
                    primary_host=pgsql_util.get_connhost(conn))
                pgsql_util.waiting_cluster_final_status(
                    meta, spec, patch, status, logger)
            logger.info(f"update configs {cmd} on %s" %
                        pgsql_util.get_connhost(conn))
            output = pgsql_util.exec_command(conn,
                                             cmd,
                                             logger,
                                             interrupt=False)
            if output.find(SUCCESS) == -1:
                logger.error(f"update configs {cmd} failed. {output}")
    if autofailover == False:
        pgsql_util.waiting_postgresql_ready(readwrite_conns, logger)
        pgsql_util.waiting_cluster_final_status(meta, spec, patch, status,
                                                logger)


def update_configs(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    AC: str,
    FIELD: Tuple,
    OLD: Any,
    NEW: Any,
) -> None:
    conns = []
    cmd = ["pgtools", "-c"]
    restart_postgresql = False
    port_change = False
    old_port = None
    new_port = None
    autofailover = False
    special_change = False

    if FIELD == DIFF_FIELD_AUTOFAILOVER_CONFIGS:
        autofailover_conns = pgsql_util.connections(
            spec, meta, patch, pgsql_util.get_field(AUTOFAILOVER), False, None,
            logger, None, status, False)
        conns += autofailover_conns.get_conns()
        autofailover = True
    elif FIELD == DIFF_FIELD_POSTGRESQL_CONFIGS:
        readwrite_conns = pgsql_util.connections(
            spec, meta, patch,
            pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE), False, None,
            logger, None, status, False)
        conns += readwrite_conns.get_conns()

        readonly_conns = pgsql_util.connections(
            spec, meta, patch,
            pgsql_util.get_field(POSTGRESQL, READONLYINSTANCE), False, None,
            logger, None, status, False)
        conns += readonly_conns.get_conns()

    if len(conns) != 0:
        for i, config in enumerate(NEW):
            name = config.split("=")[0].strip()
            value = config[config.find("=") + 1:].strip()
            if autofailover == True and name == 'port':
                continue
            if name in PG_CONFIG_IGNORE:
                continue
            if name in PG_CONFIG_RESTART:
                for oldi, oldconfig in enumerate(OLD):
                    oldname = oldconfig.split("=")[0].strip()
                    oldvalue = oldconfig[oldconfig.find("=") + 1:].strip()
                    if name == oldname and value != oldvalue:
                        logger.info(f"{name} is restart parameter ")
                        restart_postgresql = True
                        if name == 'port':
                            port_change = True
                            new_port = value
                            old_port = oldvalue
                            logger.info(
                                f"change port from {old_port} to {new_port}")
            if name in PG_CONFIG_MASTER_LARGE_THAN_SLAVE:
                for oldi, oldconfig in enumerate(OLD):
                    oldname = oldconfig.split("=")[0].strip()
                    oldvalue = oldconfig[oldconfig.find("=") + 1:].strip()
                    if name == oldname and int(value) != int(oldvalue):
                        logger.info(f"{name} must large then slave")
                        special_change = True

            config = name + '="' + value + '"'
            cmd.append('-e')
            cmd.append(PG_CONFIG_PREFIX + config)

        if autofailover == False:
            pgsql_util.waiting_postgresql_ready(readwrite_conns, logger)
            pgsql_util.waiting_postgresql_ready(readonly_conns, logger)
            pgsql_util.waiting_cluster_final_status(meta, spec, patch, status,
                                                    logger)
        #fisrst: port_change
        #second: restart_postgresql
        #third: special_change
        #bitmap: 000 001 010 011 100 101 110 111
        # if port_change is 1, restart_postgresql must be 1 can't be 0.
        # if special_change is 1, restart_postgresql must be 1 can't be 0.
        if port_change == False and restart_postgresql == False and special_change == False:
            update_configs_utile(meta, spec, patch, status, logger, conns,
                                 readwrite_conns, readonly_conns,
                                 copy.deepcopy(cmd), autofailover, False)
        if port_change == False and restart_postgresql == False and special_change == True:
            pass
        if port_change == False and restart_postgresql == True and special_change == False:
            update_configs_utile(meta, spec, patch, status, logger, conns,
                                 readwrite_conns, readonly_conns,
                                 copy.deepcopy(cmd), autofailover, True)
        if port_change == False and restart_postgresql == True and special_change == True:
            update_configs_utile(meta, spec, patch, status, logger, conns,
                                 readwrite_conns, readonly_conns,
                                 copy.deepcopy(cmd), autofailover, True)
        if port_change == True and restart_postgresql == False and special_change == False:
            pass
        if port_change == True and restart_postgresql == False and special_change == True:
            pass
        if port_change == True and restart_postgresql == True and special_change == False:
            update_configs_port(meta, spec, patch, status, logger, conns,
                                readwrite_conns, readonly_conns,
                                copy.deepcopy(cmd), autofailover)
        if port_change == True and restart_postgresql == True and special_change == True:
            # don't update port
            config = "port" + '="' + old_port + '"'
            tmpcmd = copy.deepcopy(cmd)
            tmpcmd.append('-e')
            tmpcmd.append(PG_CONFIG_PREFIX + config)
            update_configs_utile(meta, spec, patch, status, logger, conns,
                                 readwrite_conns, readonly_conns,
                                 copy.deepcopy(cmd), autofailover, True)
            # update port
            tmpcmd = copy.deepcopy(cmd)
            update_configs_port(meta, spec, patch, status, logger, conns,
                                readwrite_conns, readonly_conns,
                                copy.deepcopy(tmpcmd), autofailover)

        if port_change == True:
            pgsql_delete.delete_services(meta, spec, patch, status, logger)
            pgsql_create.create_services(meta, spec, patch, status, logger)
            # rolling update exporter env DATA_SOURCE_NAME.port
            rolling_update(meta, spec, patch, status, logger, [
                pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE),
                pgsql_util.get_field(POSTGRESQL, READONLYINSTANCE)
            ])
    if FIELD == DIFF_FIELD_AUTOFAILOVER_CONFIGS:
        autofailover_conns.free_conns()
    elif FIELD == DIFF_FIELD_POSTGRESQL_CONFIGS:
        readwrite_conns.free_conns()
        readonly_conns.free_conns()


def update_hbas(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    AC: str,
    FIELD: Tuple,
    OLD: Any,
    NEW: Any,
) -> None:
    conns = []
    cmd = ["pgtools", "-H"]

    if FIELD == DIFF_FIELD_AUTOFAILOVER_HBAS:
        autofailover_conns = pgsql_util.connections(
            spec, meta, patch, pgsql_util.get_field(AUTOFAILOVER), False, None,
            logger, None, status, False)
        conns += autofailover_conns.get_conns()
        hbas = spec[AUTOFAILOVER][HBAS]
    elif FIELD == DIFF_FIELD_POSTGRESQL_HBAS:
        readwrite_conns = pgsql_util.connections(
            spec, meta, patch,
            pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE), False, None,
            logger, None, status, False)
        conns += readwrite_conns.get_conns()

        readonly_conns = pgsql_util.connections(
            spec, meta, patch,
            pgsql_util.get_field(POSTGRESQL, READONLYINSTANCE), False, None,
            logger, None, status, False)
        conns += readonly_conns.get_conns()
        hbas = spec[POSTGRESQL][HBAS]

    if len(conns) != 0:
        for i, hba in enumerate(hbas):
            env_name = PG_HBA_PREFIX + str(i)
            env_value = hba
            cmd.append("-e")
            cmd.append(env_name + "='" + env_value + "'")

        logger.info("update hbas(" + str(cmd) + ")")
        for conn in conns:
            output = pgsql_util.exec_command(conn,
                                             cmd,
                                             logger,
                                             interrupt=False)
            if output.find(SUCCESS) == -1:
                logger.error(f"update hbas {cmd} failed. {output}")
    if FIELD == DIFF_FIELD_AUTOFAILOVER_HBAS:
        autofailover_conns.free_conns()
    elif FIELD == DIFF_FIELD_POSTGRESQL_HBAS:
        readwrite_conns.free_conns()
        readonly_conns.free_conns()


def pgpassfile_item(user: str, password: str) -> str:
    return "*:*:*:%s:%s\n" % (user, password)


def get_pgpassfile(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    users_kind: str,
) -> str:
    pgpassfile = ""
    if spec[POSTGRESQL].get(SPEC_POSTGRESQL_USERS, {}).get(users_kind) != None:
        users = spec[POSTGRESQL][SPEC_POSTGRESQL_USERS][users_kind]
        for user in users:
            pgpassfile += pgpassfile_item(
                user[SPEC_POSTGRESQL_USERS_USER_NAME],
                user[SPEC_POSTGRESQL_USERS_USER_PASSWORD])

    return pgpassfile


def update_users(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    AC: str,
    FIELD: Tuple,
    OLD: Any,
    NEW: Any,
) -> None:
    if FIELD == DIFF_FIELD_POSTGRESQL_USERS \
            or FIELD == DIFF_FIELD_POSTGRESQL_USERS_ADMIN \
            or FIELD == DIFF_FIELD_POSTGRESQL_USERS_MAINTENANCE \
            or FIELD == DIFF_FIELD_POSTGRESQL_USERS_NORMAL:
        pgsql_util.update_pgpassfile(meta, spec, patch, status, logger)

        conns = pgsql_util.connections(
            spec, meta, patch,
            pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE), False, None,
            logger, None, status, False)
        conn = pgsql_util.get_primary_conn(conns, 0, logger)
        auto_failover_conns = pgsql_util.connections(
            spec, meta, patch, pgsql_util.get_field(AUTOFAILOVER), False, None,
            logger, None, status, False)
        auto_failover_conn = auto_failover_conns.get_conns()[0]

    if AC == DIFF_ADD:
        if FIELD == DIFF_FIELD_POSTGRESQL_USERS:
            pgsql_create.create_users(meta, spec, patch, status, logger, conns)
        if FIELD == DIFF_FIELD_POSTGRESQL_USERS_ADMIN:
            pgsql_create.create_users_admin(meta, spec, patch, status, logger,
                                            conns)
        if FIELD == DIFF_FIELD_POSTGRESQL_USERS_MAINTENANCE:
            pgsql_create.create_users_maintenance(meta, spec, patch, status,
                                                  logger, conns)
        if FIELD == DIFF_FIELD_POSTGRESQL_USERS_NORMAL:
            pgsql_create.create_users_normal(meta, spec, patch, status, logger,
                                             conns)
    if AC == DIFF_REMOVE:
        users = []
        maintenance_users = []
        if FIELD == DIFF_FIELD_POSTGRESQL_USERS:
            if OLD.get(SPEC_POSTGRESQL_USERS_ADMIN) != None:
                users += OLD[SPEC_POSTGRESQL_USERS_ADMIN]
            if OLD.get(SPEC_POSTGRESQL_USERS_MAINTENANCE) != None:
                users += OLD[SPEC_POSTGRESQL_USERS_MAINTENANCE]
            if OLD.get(SPEC_POSTGRESQL_USERS_NORMAL) != None:
                users += OLD[SPEC_POSTGRESQL_USERS_NORMAL]
        if FIELD == DIFF_FIELD_POSTGRESQL_USERS_ADMIN:
            users += OLD
        if FIELD == DIFF_FIELD_POSTGRESQL_USERS_MAINTENANCE:
            users += OLD
            maintenance_users += OLD
        if FIELD == DIFF_FIELD_POSTGRESQL_USERS_NORMAL:
            users += OLD

        for user in users:
            pgsql_delete.drop_one_user(conn,
                                       user[SPEC_POSTGRESQL_USERS_USER_NAME],
                                       logger)

        for user in maintenance_users:
            pgsql_delete.drop_one_user(auto_failover_conn,
                                       user[SPEC_POSTGRESQL_USERS_USER_NAME],
                                       logger)

    if AC == DIFF_CHANGE:

        def local_change_user_password(OS: List,
                                       NS: List,
                                       maintenance_user: bool = False):
            for o in OS:
                for n in NS:
                    if o[SPEC_POSTGRESQL_USERS_USER_NAME] == n[
                            SPEC_POSTGRESQL_USERS_USER_NAME]:
                        if o[SPEC_POSTGRESQL_USERS_USER_PASSWORD] != n[
                                SPEC_POSTGRESQL_USERS_USER_PASSWORD]:
                            change_user_password(
                                conn, n[SPEC_POSTGRESQL_USERS_USER_NAME],
                                n[SPEC_POSTGRESQL_USERS_USER_PASSWORD], logger)
                            if maintenance_user:
                                change_user_password(
                                    auto_failover_conn,
                                    n[SPEC_POSTGRESQL_USERS_USER_NAME],
                                    n[SPEC_POSTGRESQL_USERS_USER_PASSWORD],
                                    logger)

        def local_drop_user(OS: List,
                            NS: List,
                            maintenance_user: bool = False):
            for o in OS:
                found = False
                for n in NS:
                    if o[SPEC_POSTGRESQL_USERS_USER_NAME] == n[
                            SPEC_POSTGRESQL_USERS_USER_NAME]:
                        found = True
                if found == False:
                    pgsql_delete.drop_one_user(
                        conn, o[SPEC_POSTGRESQL_USERS_USER_NAME], logger)
                    if maintenance_user:
                        pgsql_delete.drop_one_user(
                            auto_failover_conn,
                            o[SPEC_POSTGRESQL_USERS_USER_NAME], logger)

        def local_create_user(OS: List,
                              NS: List,
                              superuser: bool,
                              maintenance_user: bool = False):
            for n in NS:
                found = False
                for o in OS:
                    if o[SPEC_POSTGRESQL_USERS_USER_NAME] == n[
                            SPEC_POSTGRESQL_USERS_USER_NAME]:
                        found = True
                if found == False:
                    pgsql_create.create_one_user(
                        conn, n[SPEC_POSTGRESQL_USERS_USER_NAME],
                        n[SPEC_POSTGRESQL_USERS_USER_PASSWORD], superuser,
                        logger)
                    if maintenance_user:
                        pgsql_create.create_one_user(
                            auto_failover_conn,
                            n[SPEC_POSTGRESQL_USERS_USER_NAME],
                            n[SPEC_POSTGRESQL_USERS_USER_PASSWORD], superuser,
                            logger)

        if FIELD == DIFF_FIELD_POSTGRESQL_USERS:
            logger.error("UNknow diff action")
        if FIELD == DIFF_FIELD_POSTGRESQL_USERS_ADMIN:
            local_change_user_password(OLD, NEW)
            local_drop_user(OLD, NEW)
            local_create_user(OLD, NEW, True)
        if FIELD == DIFF_FIELD_POSTGRESQL_USERS_MAINTENANCE:
            local_change_user_password(OLD, NEW, True)
            local_drop_user(OLD, NEW, True)
            local_create_user(OLD, NEW, True, True)
        if FIELD == DIFF_FIELD_POSTGRESQL_USERS_NORMAL:
            local_change_user_password(OLD, NEW)
            local_drop_user(OLD, NEW)
            local_create_user(OLD, NEW, False)

    if FIELD == DIFF_FIELD_POSTGRESQL_USERS \
            or FIELD == DIFF_FIELD_POSTGRESQL_USERS_ADMIN \
            or FIELD == DIFF_FIELD_POSTGRESQL_USERS_MAINTENANCE \
            or FIELD == DIFF_FIELD_POSTGRESQL_USERS_NORMAL:
        conns.free_conns()
        auto_failover_conns.free_conns()


def get_except_nodes(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    diffs: kopf.Diff,
) -> int:
    mode, autofailover_replicas, readwrite_replicas, readonly_replicas = pgsql_util.get_replicas(
        spec)
    except_readwrite_nodes = readwrite_replicas
    except_readonly_nodes = readonly_replicas

    for diff in diffs:
        AC = diff[0]
        FIELD = diff[1]
        OLD = diff[2]
        NEW = diff[3]

        if FIELD == DIFF_FIELD_READWRITE_REPLICAS:
            if AC != DIFF_CHANGE:
                logger.error(
                    str(DIFF_FIELD_ACTION) + " only support " + DIFF_CHANGE)
            else:
                except_readwrite_nodes = OLD

        if FIELD == DIFF_FIELD_READWRITE_MACHINES:
            if AC != DIFF_CHANGE:
                logger.error(
                    str(DIFF_FIELD_ACTION) + " only support " + DIFF_CHANGE)
            else:
                except_readwrite_nodes = len(OLD)

        if FIELD == DIFF_FIELD_READONLY_REPLICAS:
            except_readonly_nodes = OLD

        if FIELD == DIFF_FIELD_READONLY_MACHINES:
            except_readonly_nodes = len(OLD)

    return except_readwrite_nodes + except_readonly_nodes


def compare_lsn(
    local_lsn: str,
    remote_lsn: str,
) -> bool:
    local_lsn = "" if local_lsn is None else local_lsn
    remote_lsn = "" if remote_lsn is None else remote_lsn

    local_list = local_lsn.split("/")
    remote_list = remote_lsn.split("/")
    if len(local_list) == 2 and len(remote_list) == 2:
        if int(local_list[0], 16) > int(remote_list[0], 16):
            return True
        elif int(local_list[0], 16) < int(remote_list[0], 16):
            return False
        elif int(local_list[0], 16) == int(remote_list[0], 16):
            if int(local_list[1], 16) >= int(remote_list[1], 16):
                return True
            else:
                return False
    return None


def get_latest_standby(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> InstanceConnection:
    readwrite_conns = pgsql_util.connections(
        spec, meta, patch, pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE),
        False, None, logger, None, status, False)

    grep_lsn_by_cmd = [
        "pg_controldata", "-D", PG_DATABASE_DIR,
        "| grep 'Latest checkpoint location' | awk '{print $4}'"
    ]
    grep_lsn_by_sql = ["pgtools", "-q", '"select pg_last_wal_receive_lsn();"']

    max_lsn = "0/0"
    max_conn = None

    for conn in readwrite_conns.get_conns():
        output = pgsql_util.exec_command(conn,
                                         WAITING_POSTGRESQL_READY_COMMAND,
                                         logger,
                                         interrupt=False)
        if output.find(FAILED) != -1:
            continue
        if output.find(POSTGRESQL_NOT_RUNNING_MESSAGE) != -1:
            temp_lsn = pgsql_util.exec_command(conn,
                                               grep_lsn_by_cmd,
                                               logger,
                                               interrupt=False)
        else:
            temp_lsn = pgsql_util.exec_command(conn,
                                               grep_lsn_by_sql,
                                               logger,
                                               interrupt=False)

        logger.info(f"conn {pgsql_util.get_connhost(conn)} lsn = {temp_lsn}")

        if compare_lsn(temp_lsn, max_lsn):
            max_lsn = temp_lsn
            max_conn = conn

    return max_conn


def promote_postgresql(meta: kopf.Meta,
                       spec: kopf.Spec,
                       patch: kopf.Patch,
                       status: kopf.Status,
                       logger: logging.Logger,
                       conn: InstanceConnection,
                       interrupt: bool = True,
                       user: str = "postgres") -> None:
    cmd = ["pg_ctl", "promote", "-D", PG_DATABASE_DIR]
    output = pgsql_util.exec_command(conn,
                                     cmd,
                                     logger,
                                     interrupt=interrupt,
                                     user=user)
    logger.info(
        f"promote_postgresql: {pgsql_util.get_connhost(conn)}, and output: {output}"
    )


def check_and_get_k8s_or_machine_param(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    param: str,
) -> (str, str, str):

    mode, _, _, _ = pgsql_util.get_replicas(spec)
    node_name = param
    logging.info(
        f"check_and_get_k8s_or_machine_param with param={node_name} execute.")

    if mode == K8S_MODE:
        temps = node_name.split(FIELD_DELIMITER)
        if node_name.find(meta['name']) == -1 or len(temps) < 3:
            raise kopf.TemporaryError(
                f"check_and_get_k8s_or_machine_param but nodeName {node_name} is not valid."
            )

        role = temps[-2]
        replica_or_machine = int(temps[-1])

        if role == AUTOFAILOVER:
            field = pgsql_util.get_field(AUTOFAILOVER)
        elif role == READWRITEINSTANCE:
            field = pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE)
        elif role == READONLYINSTANCE:
            field = pgsql_util.get_field(POSTGRESQL, READONLYINSTANCE)
    elif mode == MACHINE_MODE:
        machine = node_name
        autofailover_machines = spec.get(AUTOFAILOVER).get(MACHINES)
        readwrite_machines = spec.get(POSTGRESQL).get(READWRITEINSTANCE).get(
            MACHINES)
        readonly_machines = spec.get(POSTGRESQL).get(READONLYINSTANCE).get(
            MACHINES)

        if machine in autofailover_machines:
            field = pgsql_util.get_field(AUTOFAILOVER)
        elif machine in readwrite_machines:
            field = pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE)
        elif machine in readonly_machines:
            field = pgsql_util.get_field(POSTGRESQL, READONLYINSTANCE)

        replica_or_machine = machine

    return mode, field, replica_or_machine


def rebuild_autofailover(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    field: str,
    target_machines: List,
    target_k8s: List,  # [begin:int, end: int]
    delete_disk: bool = False,
) -> None:
    auto_failover_conns = pgsql_util.connections(
        spec, meta, patch, pgsql_util.get_field(AUTOFAILOVER), False, None,
        logger, None, status, False)

    readwrite_conns = pgsql_util.connections(
        spec, meta, patch, pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE),
        False, None, logger, None, status, False)

    readonly_conns = pgsql_util.connections(
        spec, meta, patch, pgsql_util.get_field(POSTGRESQL, READONLYINSTANCE),
        False, None, logger, None, status, False)

    primary_conns: InstanceConnections = InstanceConnections()
    standby_conns: InstanceConnections = InstanceConnections()

    pri_conn = pgsql_util.get_primary_conn(readwrite_conns,
                                           0,
                                           logger,
                                           interrupt=False)
    if pri_conn is None:
        pri_conn = get_latest_standby(meta, spec, patch, status, logger)
        if pri_conn is None:
            logger.error(f"cluster cannot find an available node")
            raise kopf.PermanentError("cluster cannot find an available node")
        promote_postgresql(meta, spec, patch, status, logger, pri_conn)
        primary_conns.add(pri_conn)
    else:
        primary_conns.add(pri_conn)

    for conn in (readwrite_conns.get_conns() + readonly_conns.get_conns()):
        if pgsql_util.get_connhost(pri_conn) != pgsql_util.get_connhost(conn):
            standby_conns.add(conn)

    # step1. pause postgresql.
    #   first, pause standby.
    #   second, pause primary.
    cmd_pause = ["pgtools", "-p", "pause"]
    cmd_restart = ["pgtools", "-R"]
    cmds = [cmd_pause, cmd_restart]

    pgsql_util.waiting_instance_ready(standby_conns, logger, 1 * MINUTES)
    pgsql_util.multi_exec_command(standby_conns, cmds, logger, interrupt=False)

    pgsql_util.waiting_instance_ready(primary_conns, logger, 1 * MINUTES)
    pgsql_util.multi_exec_command(primary_conns, cmds, logger, interrupt=False)

    # step2. update autofailover
    # rolling_update(meta, spec, patch, status, logger,
    #                [get_field(AUTOFAILOVER)], exit=True, delete_disk=True, timeout=MINUTES * 5, target_k8s=[0, 1])

    if target_machines != None:
        pgsql_delete.delete_autofailover(meta,
                                         spec,
                                         patch,
                                         status,
                                         logger,
                                         field,
                                         target_machines,
                                         None,
                                         delete_disk=delete_disk)
    else:
        pgsql_delete.delete_autofailover(meta,
                                         spec,
                                         patch,
                                         status,
                                         logger,
                                         field,
                                         None,
                                         target_k8s,
                                         delete_disk=delete_disk)
    pgsql_create.create_autofailover(meta, spec, patch, status, logger,
                                     pgsql_util.get_autofailover_labels(meta))

    # step3. resume postgresql
    #   first, resume primary.
    #   second, resume standby.
    cmd_delete = ["rm", "-rf", os.path.join(DATA_DIR, "auto_failover")]
    cmd_resume = ["pgtools", "-p", "resume"]
    cmds = [cmd_delete, cmd_resume]

    pgsql_util.waiting_instance_ready(primary_conns, logger, 1 * MINUTES)
    pgsql_util.multi_exec_command(primary_conns, cmds, logger, interrupt=False)
    pgsql_util.waiting_postgresql_ready(primary_conns, logger, 1 * MINUTES)

    pgsql_util.waiting_instance_ready(standby_conns, logger, 1 * MINUTES)
    pgsql_util.multi_exec_command(standby_conns, cmds, logger, interrupt=False)
    pgsql_util.waiting_postgresql_ready(standby_conns, logger, 1 * MINUTES)

    # step4. update number_sync_standbys
    pgsql_util.update_number_sync_standbys(meta, spec, patch, status, logger)

    auto_failover_conns.free_conns()
    readwrite_conns.free_conns()
    readonly_conns.free_conns()
    # primary_conns maybe set by get_latest_standby. readwrite_conns can't free it, free_conns can call multi times. so it is safety
    primary_conns.free_conns()
    standby_conns.free_conns()


def rebuild_postgresql(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    field: str,
    target_machines: List,
    target_k8s: List,  # [begin:int, end: int]
    delete_disk: bool = False,
) -> None:

    logger.warning(f"rebuild_postgresql wait autofailover node ready.")
    pgsql_util.waiting_target_postgresql_ready(
        meta,
        spec,
        patch,
        pgsql_util.get_field(AUTOFAILOVER),
        status,
        logger,
        exit=True,
        timeout=MINUTES * 1)

    conns = pgsql_util.connections_target(meta, spec, patch, status, logger,
                                          field, target_machines, target_k8s)

    if field == pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE):
        instance = READWRITEINSTANCE
    elif field == pgsql_util.get_field(POSTGRESQL, READONLYINSTANCE):
        instance = READONLYINSTANCE

    for conn in conns.get_conns():
        primary_host = pgsql_util.get_primary_host(meta, spec, patch, status,
                                                   logger)
        conn_host = pgsql_util.get_connhost(conn)

        logger.info(f"rebuild_postgresql start rebuild {conn_host}.")
        if conn_host == primary_host:
            logger.warning(f"{conn_host} is primary host, skip rebuild.")
            continue

        # rolling_update(meta, spec, patch, status, logger,
        #                [field], exit=True, delete_disk=delete_disk, timeout=MINUTES * 5, target_k8s=[replica, replica + 1])
        if target_machines != None:
            for replica in range(0, len(target_machines)):
                if instance == READWRITEINSTANCE:
                    pgsql_delete.delete_postgresql_readwrite(
                        meta, spec, patch, status, logger, field,
                        target_machines[replica:replica + 1], None,
                        delete_disk)
                    machine_replica = pgsql_util.get_replica_by_machine(
                        spec, field, target_machines[replica])
                    pgsql_create.create_postgresql_readwrite(
                        meta, spec, patch, status, logger,
                        pgsql_util.get_readwrite_labels(meta), machine_replica,
                        False, machine_replica + 1)
                elif instance == READONLYINSTANCE:
                    pgsql_delete.delete_postgresql_readonly(
                        meta, spec, patch, status, logger, field,
                        target_machines[replica:replica + 1], None,
                        delete_disk)
                    machine_replica = pgsql_util.get_replica_by_machine(
                        spec, field, target_machines[replica])
                    pgsql_create.create_postgresql_readonly(
                        meta, spec, patch, status, logger,
                        pgsql_util.get_readonly_labels(meta), machine_replica,
                        machine_replica + 1)
        else:
            for replica in range(target_k8s[0], target_k8s[1]):
                if instance == READWRITEINSTANCE:
                    pgsql_delete.delete_postgresql_readwrite(
                        meta, spec, patch, status, logger, field, None,
                        [replica, replica + 1], delete_disk)
                    pgsql_create.create_postgresql_readwrite(
                        meta, spec, patch, status, logger,
                        pgsql_util.get_readwrite_labels(meta), replica, False,
                        replica + 1)
                elif instance == READONLYINSTANCE:
                    pgsql_delete.delete_postgresql_readonly(
                        meta, spec, patch, status, logger, field, None,
                        [replica, replica + 1], delete_disk)
                    pgsql_create.create_postgresql_readonly(
                        meta, spec, patch, status, logger,
                        pgsql_util.get_readonly_labels(meta), replica,
                        replica + 1)

    conns.free_conns()


def rebuild_postgresqls(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    delete_disk: bool = False,
) -> None:

    node_name = spec.get(SPEC_REBUILD,
                         {}).get(SPCE_REBUILD_NODENAMES,
                                 "").split(SPECIAL_CHARACTERS)[0].strip()

    mode, field, replica_or_machine = check_and_get_k8s_or_machine_param(
        meta, spec, patch, status, logger, node_name)

    if mode == K8S_MODE:
        if field == pgsql_util.get_field(AUTOFAILOVER):
            rebuild_autofailover(meta,
                                 spec,
                                 patch,
                                 status,
                                 logger,
                                 field,
                                 None, [0, 1],
                                 delete_disk=True)
        elif field in [
                pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE),
                pgsql_util.get_field(POSTGRESQL, READONLYINSTANCE)
        ]:
            rebuild_postgresql(meta, spec, patch, status, logger, field, None,
                               [replica_or_machine, replica_or_machine + 1],
                               delete_disk)

    elif mode == MACHINE_MODE:
        if field == pgsql_util.get_field(AUTOFAILOVER):
            rebuild_autofailover(meta, spec, patch, status, logger, field,
                                 [replica_or_machine], None, delete_disk)
        elif field in [
                pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE),
                pgsql_util.get_field(POSTGRESQL, READONLYINSTANCE)
        ]:
            rebuild_postgresql(meta, spec, patch, status, logger, field,
                               [replica_or_machine], None, delete_disk)


def switchover_to_target_readwrite(
        meta: kopf.Meta,
        spec: kopf.Spec,
        patch: kopf.Patch,
        status: kopf.Status,
        logger: logging.Logger,
        field: str,
        target_machines: List,
        target_k8s: List,  # [begin:int, end: int]
) -> None:

    if field in [
            pgsql_util.get_field(AUTOFAILOVER),
            pgsql_util.get_field(POSTGRESQL, READONLYINSTANCE)
    ]:
        logger.error(f"cannot switchover to {field}.")
        raise kopf.PermanentError(f"cannot switchover to {field}.")

    logger.warning(
        f"switchover_to_target_readwrite wait autofailover node ready.")
    pgsql_util.waiting_target_postgresql_ready(
        meta,
        spec,
        patch,
        pgsql_util.get_field(AUTOFAILOVER),
        status,
        logger,
        exit=True,
        timeout=MINUTES * 1)

    # connect to target k8s or target machine
    conns = pgsql_util.connections_target(meta, spec, patch, status, logger,
                                          field, target_machines, target_k8s)

    for conn in conns.get_conns():
        primary_host = pgsql_util.get_primary_host(meta, spec, patch, status,
                                                   logger)
        conn_host = pgsql_util.get_connhost(conn)

        if conn_host == primary_host:
            logger.warning(f"{conn_host} is primary host, skip switchover.")
            continue

        logger.info(
            f"switchover_to_target_readwrite start switchover master to {conn_host}."
        )

        # update target readwrite priority to high
        update_node_priority(meta, spec, patch, status, logger, [conn],
                             NODE_PRIORITY_HIGH, primary_host)

        pgsql_util.autofailover_switchover(meta, spec, patch, status, logger)

        # update target readwrite priority to normal
        update_node_priority(meta, spec, patch, status, logger, [conn],
                             NODE_PRIORITY_DEFAULT)

    conns.free_conns()


def switchover_postgresqls(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> None:

    masterNode = spec.get(SPEC_SWITCHOVER,
                          {}).get(SPEC_SWITCHOVER_MASTERNODE,
                                  "").split(SPECIAL_CHARACTERS)[0].strip()
    mode, field, replica_or_machine = check_and_get_k8s_or_machine_param(
        meta, spec, patch, status, logger, masterNode)

    if mode == K8S_MODE:
        switchover_to_target_readwrite(
            meta, spec, patch, status, logger, field, None,
            [replica_or_machine, replica_or_machine + 1])
    elif mode == MACHINE_MODE:
        switchover_to_target_readwrite(meta, spec, patch, status, logger,
                                       field, [replica_or_machine], None)


def change_user_password(conn: InstanceConnection, name: str, password: str,
                         logger: logging.Logger) -> None:
    cmd = "alter user " + name + " "
    cmd = cmd + " password '" + password + "'"
    cmd = ["pgtools", "-q", '"' + cmd + '"']

    logger.info(f"alter postgresql user with cmd {cmd}")
    output = pgsql_util.exec_command(conn, cmd, logger, interrupt=False)
    if output != "ALTER ROLE":
        logger.error(f"can't alter user {cmd}, {output}")


def retain_postgresql_data(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    conn: InstanceConnection,
) -> None:
    # don't free the tmpconns
    tmpconns: InstanceConnections = InstanceConnections()
    tmpconns.add(conn)

    # wait postgresql ready
    pgsql_util.waiting_postgresql_ready(tmpconns, logger, timeout=MINUTES * 5)

    # drop from autofailover and pause start postgresql
    cmd = ["pgtools", "-d", "-p", POSTGRESQL_PAUSE]
    pgsql_util.exec_command(conn, cmd, logger, interrupt=True)

    pgsql_util.waiting_instance_ready(tmpconns, logger)

    # remove old data
    cmd = [
        "rm", "-rf", DATA_DIR + "/" + DIR_ASSIST,
        DATA_DIR + "/" + DIR_AUTO_FAILOVER, DATA_DIR + "/" + DIR_BACKUP,
        DATA_DIR + "/" + DIR_BARMAN
    ]
    pgsql_util.exec_command(conn, cmd, logger, interrupt=True)

    # delay start
    cmd = ["touch", ASSIST_DIR + "/stop"]
    #cmd = ["pgtools", "-R"]
    pgsql_util.exec_command(conn, cmd, logger, interrupt=True)

    # remove recovery file
    #cmd =["rm", "-rf", os.path.join(PG_DATABASE_DIR, RECOVERY_CONF_FILE)]
    cmd = [
        "truncate", "--size", "0",
        os.path.join(PG_DATABASE_DIR, RECOVERY_CONF_FILE)
    ]
    pgsql_util.exec_command(conn, cmd, logger, interrupt=True, user='postgres')

    #cmd =["rm", "-rf", os.path.join(PG_DATABASE_DIR, RECOVERY_SET_FILE)]
    cmd = [
        "truncate", "--size", "0",
        os.path.join(PG_DATABASE_DIR, RECOVERY_SET_FILE)
    ]
    pgsql_util.exec_command(conn, cmd, logger, interrupt=True, user='postgres')

    cmd = ["rm", "-rf", os.path.join(PG_DATABASE_DIR, STANDBY_SIGNAL)]
    pgsql_util.exec_command(conn, cmd, logger, interrupt=True)

    # resume postgresql
    cmd = ["pgtools", "-p", POSTGRESQL_RESUME]
    pgsql_util.exec_command(conn, cmd, logger, interrupt=True)


def update_disasterBackup(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    AC: str,
    FIELD: Tuple,
    OLD: Any,
    NEW: Any,
) -> None:
    if FIELD[0:len(DIFF_FIELD_DISASTERBACKUP)] == DIFF_FIELD_DISASTERBACKUP:

        def enable_disaster():
            conns = []
            readwrite_conns = pgsql_util.connections(
                spec, meta, patch,
                pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE), False,
                None, logger, None, status, False)
            conns += readwrite_conns.get_conns()
            readonly_conns = pgsql_util.connections(
                spec, meta, patch,
                pgsql_util.get_field(POSTGRESQL, READONLYINSTANCE), False,
                None, logger, None, status, False)
            conns += readonly_conns.get_conns()
            for conn in conns:
                pgsql_delete.delete_postgresql(meta,
                                               spec,
                                               patch,
                                               status,
                                               logger,
                                               True,
                                               conn,
                                               switchover=False)
            readwrite_conns.free_conns()
            readonly_conns.free_conns()
            pgsql_create.create_postgresql_disaster(meta, spec, patch, status,
                                                    logger)
            if spec[SPEC_DISASTERBACKUP][
                    SPEC_DISASTERBACKUP_STREAMING] == STREAMING_SYNC:
                pgsql_util.update_number_sync_standbys(meta, spec, patch,
                                                       status, logger)

        def disable_disaster():
            readwrite_conns = pgsql_util.connections(
                spec, meta, patch,
                pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE), False,
                None, logger, None, status, False)
            pgsql_util.create_ssl_key(meta,
                                      spec,
                                      patch,
                                      status,
                                      logger,
                                      conn=readwrite_conns.get_conns()[0])
            if spec[SPEC_DISASTERBACKUP][
                    SPEC_DISASTERBACKUP_STREAMING] == STREAMING_SYNC:
                pgsql_util.update_number_sync_standbys(meta,
                                                       spec,
                                                       patch,
                                                       status,
                                                       logger,
                                                       is_delete=True,
                                                       force_disaster=True)
            retain_postgresql_data(meta, spec, patch, status, logger,
                                   readwrite_conns.get_conns()[0])
            pgsql_delete.delete_postgresql(meta,
                                           spec,
                                           patch,
                                           status,
                                           logger,
                                           False,
                                           readwrite_conns.get_conns()[0],
                                           switchover=False)
            readwrite_conns.free_conns()
            pgsql_create.create_postgresql_readwrite(
                meta, spec, patch, status, logger,
                pgsql_util.get_readwrite_labels(meta), 0, False)
            pgsql_create.create_postgresql_readonly(
                meta, spec, patch, status, logger,
                pgsql_util.get_readonly_labels(meta), 0)
            pgsql_util.waiting_cluster_final_status(meta,
                                                    spec,
                                                    patch,
                                                    status,
                                                    logger,
                                                    timeout=MINUTES * 5)

        if AC == DIFF_ADD and pgsql_util.in_disaster_backup(
                meta, spec, patch, status, logger) == True:
            enable_disaster()
        if AC == DIFF_REMOVE:
            disable_disaster()
        if AC == DIFF_CHANGE:
            if FIELD[-1] == SPEC_DISASTERBACKUP_ENABLE:
                if NEW == True:  # enable: false to true
                    enable_disaster()
                if NEW == False:  # enable: false to true
                    disable_disaster()
            else:
                if spec[SPEC_DISASTERBACKUP][
                        SPEC_DISASTERBACKUP_ENABLE] == False:  # other field eg sync but enable is true.
                    return
                else:
                    readwrite_conns = pgsql_util.connections(
                        spec, meta, patch,
                        pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE),
                        False, None, logger, None, status, False)
                    if FIELD[-1] == SPEC_DISASTERBACKUP_STREAMING:
                        # if disaster mode running, don't have node in autofailover.
                        if len(
                                pgsql_util.get_primary_host(
                                    meta, spec, patch, status, logger)) == 0:
                            set_postgresql_streaming(
                                meta,
                                spec,
                                patch,
                                status,
                                logger,
                                NEW, [readwrite_conns.get_conns()[0]],
                                update_sync=True,
                                force_disaster=True)
                        readwrite_conns.free_conns()
                    else:
                        logger.error(
                            f"don't support change {FIELD[-1]} when disaster backup is running"
                        )
                        #pgsql_delete.delete_postgresql(meta, spec, patch, status,
                        #                             logger, True, readwrite_conns.get_conns()[0], switchover = False)
                        #readwrite_conns.free_conns()
                        #pgsql_create.create_postgresql_disaster(
                        #        meta, spec, patch, status, logger)


# kubectl patch pg lzzhang --patch '{"spec": {"action": "stop"}}' --type=merge
def update_cluster(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    diffs: kopf.Diff,
) -> None:
    try:
        logger.info("check update_cluster params")
        pgsql_util.check_param(meta, spec, patch, status, logger, create=False)

        pgsql_util.set_cluster_status(meta, CLUSTER_STATE,
                                      CLUSTER_STATUS_UPDATE, logger)

        def update_common():
            for diff in diffs:
                AC = diff[0]
                FIELD = diff[1]
                OLD = diff[2]
                NEW = diff[3]

                logger.info(diff)

                update_disasterBackup(meta, spec, patch, status, logger, AC,
                                      FIELD, OLD, NEW)

            if pgsql_util.in_disaster_backup(meta, spec, patch, status,
                                             logger) == True:
                logger.warning(
                    "disaster mode only allow modify disasterBackup")
                return

            need_roll_update = False
            need_update_number_sync_standbys = False
            update_toleration = spec.get(UPDATE_TOLERATION, False)
            except_nodes = get_except_nodes(meta, spec, patch, status, logger,
                                            diffs)

            for diff in diffs:
                AC = diff[0]
                FIELD = diff[1]
                OLD = diff[2]
                NEW = diff[3]

                update_action(meta, spec, patch, status, logger, AC, FIELD,
                              OLD, NEW)
                update_service(meta, spec, patch, status, logger, AC, FIELD,
                               OLD, NEW)
                trigger_rebuild_postgresql(meta, spec, patch, status, logger,
                                           AC, FIELD, OLD, NEW)

            if update_toleration == False and pgsql_util.waiting_cluster_final_status(
                    meta, spec, patch, status, logger,
                    except_nodes=except_nodes) == False:
                logger.error(f"cluster status is not health.")
                raise kopf.PermanentError(f"cluster status is not health.")
            else:
                for diff in diffs:
                    AC = diff[0]
                    FIELD = diff[1]
                    OLD = diff[2]
                    NEW = diff[3]

                    return_update_number_sync_standbys = update_replicas(
                        meta, spec, patch, status, logger, AC, FIELD, OLD, NEW)
                    if need_update_number_sync_standbys == False and return_update_number_sync_standbys == True:
                        need_update_number_sync_standbys = True

            for diff in diffs:
                AC = diff[0]
                FIELD = diff[1]
                OLD = diff[2]
                NEW = diff[3]

                if update_toleration == False and pgsql_util.waiting_cluster_final_status(
                        meta, spec, patch, status, logger) == False:
                    logger.error(f"cluster status is not health.")
                    raise kopf.PermanentError(f"cluster status is not health.")

                update_podspec_volume(meta, spec, patch, status, logger, AC,
                                      FIELD, OLD, NEW)
                if FIELD[0:len(DIFF_FIELD_SPEC_ANTIAFFINITY
                               )] == DIFF_FIELD_SPEC_ANTIAFFINITY:
                    need_roll_update = True

            if need_roll_update:
                update_antiaffinity(
                    meta,
                    spec,
                    patch,
                    status,
                    logger, [
                        pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE),
                        pgsql_util.get_field(POSTGRESQL, READONLYINSTANCE)
                    ],
                    True,
                    timeout=MINUTES * 5)

            for diff in diffs:
                AC = diff[0]
                FIELD = diff[1]
                OLD = diff[2]
                NEW = diff[3]

                if update_toleration == False and pgsql_util.waiting_cluster_final_status(
                        meta, spec, patch, status, logger) == False:
                    logger.error(f"cluster status is not health.")
                    raise kopf.PermanentError(f"cluster status is not health.")

                update_hbas(meta, spec, patch, status, logger, AC, FIELD, OLD,
                            NEW)
                update_users(meta, spec, patch, status, logger, AC, FIELD, OLD,
                             NEW)
                return_update_number_sync_standbys = update_streaming(
                    meta, spec, patch, status, logger, AC, FIELD, OLD, NEW)
                if need_update_number_sync_standbys == False and return_update_number_sync_standbys == True:
                    need_update_number_sync_standbys = True
                update_configs(meta, spec, patch, status, logger, AC, FIELD,
                               OLD, NEW)

                trigger_backup_to_s3_manual(meta, spec, patch, status, logger,
                                            AC, FIELD, OLD, NEW)
                trigger_switchover_postgresql(meta, spec, patch, status,
                                              logger, AC, FIELD, OLD, NEW)

            # waiting
            if spec[ACTION] == ACTION_START:
                logger.info("waiting for update_cluster success")
                pgsql_util.waiting_cluster_final_status(
                    meta, spec, patch, status, logger)

                # after waiting_cluster_final_status. update number_sync
                if need_update_number_sync_standbys:
                    pgsql_util.waiting_cluster_final_status(meta,
                                                            spec,
                                                            patch,
                                                            status,
                                                            logger,
                                                            timeout=MINUTES *
                                                            5)
                    pgsql_util.update_number_sync_standbys(
                        meta, spec, patch, status, logger)

            # wait a few seconds to prevent the pod not running
            time.sleep(5)

        update_common()

        if spec[ACTION] == ACTION_STOP:
            cluster_status = CLUSTER_STATUS_STOP
        else:
            cluster_status = CLUSTER_STATUS_RUN
        # set Running
        pgsql_util.set_cluster_status(meta, CLUSTER_STATE, cluster_status,
                                      logger)
    except Exception as e:
        logger.error(f"error occurs, {e}")
        traceback.print_exc()
        traceback.format_exc()
        pgsql_util.set_cluster_status(meta, CLUSTER_STATE,
                                      CLUSTER_STATUS_UPDATE_FAILED, logger)
