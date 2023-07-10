import logging
import kopf

from pgsqlcommons.typed import TypedDict, InstanceConnections
from pgsqlcommons.constants import *
import pgsqlclusters.utiles as pgsql_util
import pgsqlclusters.update as pgsql_update
import pgsqlbackups.utils as backup_util
from pgsqlbackups.constants import *


def backup_postgresql_to_s3(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    backup_alias_name: str,
) -> None:

    logger.info(f"backup cluster to s3")
    # get readwrite connections
    conns = pgsql_util.connections(
        spec, meta, patch, pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE),
        False, None, logger, None, status, False)
    # wait postgresql ready
    pgsql_util.waiting_postgresql_ready(conns, logger)

    # add s3 env by pgtools
    s3_info = backup_util.get_need_s3_env(
        meta, spec, patch, status, logger,
        [SPEC_S3, SPEC_BACKUPTOS3_POLICY, BACKUP_NAME])
    # set backup alias name
    alias_name = [
        '-e', BARMAN_BACKUP_ALIAS_NAME_KEY + '="' + backup_alias_name + '"'
    ]
    cmd = ["pgtools", "-b"] + s3_info + alias_name
    logging.warning(
        f"backup_postgresql_to_s3 execute {cmd} to backup cluster on readwrite node"
    )

    for conn in conns.get_conns():
        # backup
        output = pgsql_util.exec_command(conn,
                                         cmd,
                                         logger,
                                         interrupt=True,
                                         user="postgres")
        if output.find(SUCCESS) == -1:
            logger.error(f"execute {cmd} failed. {output}")
            # raise TemporaryError and retry by kopf
            raise kopf.TemporaryError("backup cluster to s3 failed.")

    conns.free_conns()


##########
# default: delete backup by rentention
# backup_id=delete_all, delete all backup, use by delete cluster
# backup_id=20230627T120854, delete 20230627T120854 backup
##########
def delete_s3_backup(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    backup_id: str = None,
) -> None:

    s3_info = backup_util.get_need_s3_env(
        meta, spec, patch, status, logger,
        [SPEC_S3, SPEC_BACKUPTOS3_POLICY, BACKUP_NAME])

    env = None
    if backup_id == SPEC_BACKUPTOS3_POLICY_RETENTION_DELETE_ALL_VALUE:
        # override rentention and delete all backup
        env = SPEC_BACKUPTOS3_POLICY_RETENTION + '="' + \
                SPEC_BACKUPTOS3_POLICY_RETENTION_DELETE_ALL_VALUE + '"'
    elif backup_id is not None:
        env = BARMAN_BACKUP_ID + '="' + backup_id + '"'
    if env is not None:
        s3_info.append('-e')
        s3_info.append(env)

    readwrite_conns = pgsql_util.connections(
        spec, meta, patch, pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE),
        False, None, logger, None, status, False)
    pgsql_util.waiting_instance_ready(readwrite_conns,
                                      logger,
                                      timeout=1 * MINUTES)
    for conn in readwrite_conns.get_conns():
        cmd = ["pgtools", "-B"] + s3_info
        output = pgsql_util.exec_command(conn,
                                         cmd,
                                         logger,
                                         interrupt=True,
                                         user="postgres")
        logger.warning(
            f"delete backup execute on {pgsql_util.get_connhost(conn)}, and output = {output}"
        )

    readwrite_conns.free_conns()


def delete_s3_backup_by_backup_cr(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    backup_id: str,
    backup_params: TypedDict,
) -> None:
    if backup_util.is_s3_backup_mode(backup_params):
        delete_s3_backup(meta,
                         spec,
                         patch,
                         status,
                         logger,
                         backup_id=backup_id)


def delete_s3_backup_by_main_cr(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    backup_id: str = None,
) -> None:
    if backup_util.get_backup_mode(
            meta, spec, patch, status,
            logger) == BACKUP_MODE_S3_MANUAL or backup_util.get_backup_mode(
                meta, spec, patch, status, logger) == BACKUP_MODE_S3_CRON:
        delete_s3_backup(meta,
                         spec,
                         patch,
                         status,
                         logger,
                         backup_id=backup_id)


def set_backup_status_by_backup_cr(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    conns: InstanceConnections,
    env: TypedDict,
    backup_params: TypedDict,
    backup_alias_name: str,
):
    old_backup_status = status.get(CLUSTER_STATUS_BACKUP, list())
    new_backup_status, _ = backup_util.get_s3_backup_list(
        meta, spec, patch, status, logger)

    logger.warning(f"old_backup_status = {old_backup_status}")
    logger.warning(f"new_backup_status = {new_backup_status}")

    backup_list_status = old_backup_status
    res_status = dict()
    for new_status in new_backup_status:
        backup_id = new_status[BARMAN_BACKUP_ID]

        error = new_status.pop(BARMAN_BACKUP_ERROR, None)
        alias_name = new_status.pop(BARMAN_BACKUP_ALIAS_NAME, None)

        # only add current backup to backup_list_status
        if alias_name != backup_alias_name:
            continue
        if error is not None:
            raise kopf.TemporaryError(
                f"set_backup_status_by_backup_cr failed, err is {error}.")

        param = 'BACKUP_ID' + '="' + backup_id + '"'
        cmd = ["pgtools", "-v", "-e", param] + env
        size = pgsql_util.exec_command(conns.get_conns()[0],
                                       cmd,
                                       logger,
                                       interrupt=True,
                                       user="postgres")
        new_status[BARMAN_BACKUP_SIZE] = size.strip()

        backup_list_status.append(new_status)
        res_status = new_status

    backup_util.set_display_backup_status(
        {
            METADATA_NAME: backup_params[METADATA_NAME],
            METADATA_NAMESPACE: backup_params[METADATA_NAMESPACE]
        },
        res_status,
        logger,
        other_status={BARMAN_BACKUP_LISTS: backup_list_status})


def after_backup_postgresql_to_s3_by_backup_cr(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    backup_params: TypedDict,
    backup_alias_name: str,
) -> None:
    conns = pgsql_util.connections(
        spec, meta, patch, pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE),
        False, None, logger, None, status, False)
    s3_info = backup_util.get_need_s3_env(
        meta, spec, patch, status, logger,
        [SPEC_S3, SPEC_BACKUPTOS3_POLICY, BACKUP_NAME])

    delete_s3_backup(meta, spec, patch, status, logger)
    set_backup_status_by_backup_cr(meta, spec, patch, status, logger, conns,
                                   s3_info, backup_params, backup_alias_name)

    conns.free_conns()


def backup_postgresql_by_backup_cr(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    backup_params: TypedDict,
) -> None:
    if backup_util.is_s3_backup_mode(backup_params):
        backup_alias_name = backup_util.generate_backup_alias_name(
            backup_params[METADATA_NAME], backup_params[METADATA_NAMESPACE])
        backup_postgresql_to_s3(meta, spec, patch, status, logger,
                                backup_alias_name)
        after_backup_postgresql_to_s3_by_backup_cr(meta, spec, patch, status,
                                                   logger, backup_params,
                                                   backup_alias_name)

    logger.info(f"backup_postgresql cluster success.")
    return


def delete_postgresql_backup(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    backup_params: TypedDict,
) -> None:
    if backup_params[SPEC_DELETES3]:
        backup_list = status.get(CLUSTER_STATUS_BACKUP, None)
        backups = list()
        for backup in backup_list:
            backup_id = backup[BARMAN_BACKUP_ID]
            if not backup_util.is_backup_id(backup_id):
                logger.warning(f"backup_id {backup_id} not valid, skip ...")
                continue
            backups.append(backup_id)

        delete_s3_backup_by_backup_cr(meta, spec, patch, status, logger,
                                      BACKUP_ID_SPLIT_CHARACTERS.join(backups),
                                      backup_params)


def check_backup_valid(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    backup_params: TypedDict,
) -> None:
    if backup_util.is_s3_backup_mode(backup_params):
        logger.info(
            "check_backup_valid will correct backup_list and delete invalid backup."
        )
        old_backup_status = status.get(CLUSTER_STATUS_BACKUP, list())
        new_backup_status, latest_backup_id = backup_util.get_s3_backup_list(
            meta, spec, patch, status, logger)

        backup_ids = list()
        for backup in new_backup_status:
            backup_id = backup[BARMAN_BACKUP_ID]
            if not backup_util.is_backup_id(backup_id):
                logger.warning(f"backup_id {backup_id} not valid, skip ...")
                continue
            backup_ids.append(backup_id)

        logger.info(
            f"check_backup_valid get old_backup_status = {old_backup_status}")
        logger.info(
            f"check_backup_valid get new_backup_status = {new_backup_status}")
        logger.info(
            f"check_backup_valid get S3 backup_id_list is {backup_ids}")

        res = list()
        for backup in old_backup_status:
            backup_id = backup[BARMAN_BACKUP_ID]
            if backup_id not in backup_ids:
                logger.warning(
                    f"backup_id {backup_id} not in S3 backup list, will delete it ..."
                )
                continue
            res.append(backup)

        name = backup_params[METADATA_NAME]
        namespace = backup_params[METADATA_NAMESPACE]
        # backup cr will be deleted only if the single backup invalid
        if len(res) == 0 and backup_params[
                SPEC_BACKUP_BACKUPTOS3_KIND] == SPEC_BACKUP_BACKUPTOS3_KIND_SIGNAL:
            logger.warning(
                f"check_backup_valid will delete {RESOURCE_POSTGRESQL_BACKUP} with name {name}, namespace {namespace}"
            )
            if backup_params[SPEC_DELETES3]:
                backup_util.set_backup_status(
                    {
                        METADATA_NAME: name,
                        METADATA_NAMESPACE: namespace
                    }, SPEC_DELETES3, False, logger)
            backup_util.delete_backup_cr(name, namespace, logger)
        elif len(res) < len(old_backup_status):
            logger.warning(
                f"some backup maybe invalid, check_backup_valid will correct {RESOURCE_POSTGRESQL_BACKUP} with name {name}, namespace {namespace} .{STATUS}.{BARMAN_BACKUP_LISTS} field."
            )
            backup_util.set_display_backup_status(
                {
                    METADATA_NAME: name,
                    METADATA_NAMESPACE: namespace
                },
                res[-1] if len(res) > 0 else dict(),
                logger,
                other_status={BARMAN_BACKUP_LISTS: res})


#########################################
# main cr
# #########################################
def set_backup_status_by_main_cr(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    conns: InstanceConnections,
    env: TypedDict,
):
    old_backup_status = status.get(CLUSTER_STATUS_BACKUP, None)
    new_backup_status, latest_backup_id = backup_util.get_s3_backup_list(
        meta, spec, patch, status, logger)

    logger.warning(f"old_backup_status = {old_backup_status}")
    logger.warning(f"new_backup_status = {new_backup_status}")
    logger.warning(f"latest_backup_id = {latest_backup_id}")

    backup_list_status = list()
    for new_status in new_backup_status:
        # old backup
        is_old_backup = False
        if old_backup_status is not None:
            for old_status in old_backup_status:
                if new_status[BARMAN_BACKUP_ID] == old_status[
                        BARMAN_BACKUP_ID]:
                    backup_list_status.append(old_status)
                    is_old_backup = True
                    break
        if is_old_backup:
            continue
        # if s3 backup, we can add size field
        backup_id = new_status[BARMAN_BACKUP_ID]
        param = 'BACKUP_ID' + '="' + backup_id + '"'
        cmd = ["pgtools", "-v", "-e", param] + env
        size = pgsql_util.exec_command(conns.get_conns()[0],
                                       cmd,
                                       logger,
                                       interrupt=True,
                                       user="postgres")
        new_status[BARMAN_BACKUP_SIZE] = size.strip()

        # if latest backup, we can add some snapshot infomation in k8s mode
        mode, _, _, _ = pgsql_util.get_replicas(spec)
        if mode == K8S_MODE and latest_backup_id == backup_id:
            # get cpu/memory/replicas/pvc_size/pvc_class
            for container in spec[POSTGRESQL][READWRITEINSTANCE][PODSPEC][
                    CONTAINERS]:
                if container[
                        CONTAINER_NAME] == PODSPEC_CONTAINERS_POSTGRESQL_CONTAINER:
                    cpu = container["resources"]["limits"][
                        SPEC_POSTGRESQL_READWRITE_RESOURCES_LIMITS_CPU]
                    memory = container["resources"]["limits"][
                        SPEC_POSTGRESQL_READWRITE_RESOURCES_LIMITS_MEMORY]
            replicas = spec[POSTGRESQL][READWRITEINSTANCE][REPLICAS]

            for vct in spec[POSTGRESQL][READWRITEINSTANCE][
                    VOLUMECLAIMTEMPLATES]:
                if vct["metadata"]["name"] == POSTGRESQL_PVC_NAME:
                    pvc_size = pgsql_update.get_vct_size(vct)
                    storage_class_name = vct["spec"].get(
                        STORAGE_CLASS_NAME, "")

            new_status[BARMAN_BACKUP_SNAPSHOT_CPU] = cpu
            new_status[BARMAN_BACKUP_SNAPSHOT_MEMORY] = memory
            new_status[BARMAN_BACKUP_SNAPSHOT_REPLICAS] = replicas
            new_status[BARMAN_BACKUP_SNAPSHOT_PVC_SIZE] = pvc_size
            new_status[BARMAN_BACKUP_SNAPSHOT_PVC_CLASS] = storage_class_name
        backup_list_status.append(new_status)

    # set backup status
    if len(backup_list_status) > 0:
        pgsql_util.set_cluster_status(meta, CLUSTER_STATUS_BACKUP,
                                      backup_list_status, logger)


def after_backup_postgresql_to_s3_by_main_cr(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> None:
    conns = pgsql_util.connections(
        spec, meta, patch, pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE),
        False, None, logger, None, status, False)
    s3_info = backup_util.get_need_s3_env(
        meta, spec, patch, status, logger,
        [SPEC_S3, SPEC_BACKUPTOS3_POLICY, BACKUP_NAME])

    delete_s3_backup(meta, spec, patch, status, logger)
    set_backup_status_by_main_cr(meta, spec, patch, status, logger, conns,
                                 s3_info)

    conns.free_conns()


def backup_postgresql_by_main_cr(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> None:
    if backup_util.get_backup_mode(
            meta, spec, patch, status,
            logger) == BACKUP_MODE_S3_MANUAL or backup_util.get_backup_mode(
                meta, spec, patch, status, logger) == BACKUP_MODE_S3_CRON:
        backup_alias_name = backup_util.generate_backup_alias_name(
            meta[METADATA_NAME],
            meta[METADATA_NAMESPACE],
        )
        backup_postgresql_to_s3(meta, spec, patch, status, logger,
                                backup_alias_name)
        after_backup_postgresql_to_s3_by_main_cr(meta, spec, patch, status,
                                                 logger)
    logger.info(f"backup_postgresql cluster success.")
    return
