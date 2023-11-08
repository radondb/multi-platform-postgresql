import time
import kopf
import logging
import copy
import json

from kubernetes import client

from pgsqlcommons.constants import *
from pgsqlcommons.typed import TypedDict, List, Any
from pgsqlbackups.constants import *
import pgsqlclusters.utiles as pgsql_util


def is_backup_id(backupid: str) -> bool:
    try:
        time.strptime(backupid, "%Y%m%dT%H%M%S")
        return True
    except:
        return False


def valid_date_format(date: str) -> str:
    res = None
    for format in SUPPORTED_DATE_FORMAT:
        try:
            time.strptime(date, format)
            res = format
            break
        except:
            continue
    return res


def compare_timestamp(t1: str, t2: str) -> str:
    if int(t1) >= int(t2):
        return t1
    else:
        return t2


"""
begin_time: "Mon Sep 26 16:17:13 2022"
begin_wal:  "000000050000000000000010"
end_time:   "Mon Sep 26 16:17:23 2022"
end_wal:    "000000050000000000000010"
backup_id:  "20220926T081713"
"""


def get_backupid_from_backupinfo(recovery_time: str,
                                 backup_info: TypedDict) -> str:
    backupid = None
    date_format = valid_date_format(recovery_time)
    if date_format is None:
        return backupid
    recovery_timestamp = time.mktime(time.strptime(recovery_time, date_format))
    max_require_timestamp = None

    backup_info = backup_info[BARMAN_BACKUP_LISTS]
    for backup in backup_info:
        temp = time.mktime(
            time.strptime(backup[BARMAN_BACKUP_END], BARMAN_TIME_FORMAT))
        # backup end_time more than recovery_time
        if compare_timestamp(recovery_timestamp, temp) == temp:
            continue
        elif compare_timestamp(recovery_timestamp, temp) == recovery_timestamp:
            if max_require_timestamp is None or compare_timestamp(
                    max_require_timestamp, temp) == temp:
                max_require_timestamp = temp
                backupid = backup[BARMAN_BACKUP_ID]

    return backupid


def get_latest_backupid(backup_info: TypedDict) -> str:
    backupid = None
    max_timestamp = 0

    backup_info = backup_info[BARMAN_BACKUP_LISTS]
    for backup in backup_info:
        temp = time.mktime(
            time.strptime(backup[BARMAN_BACKUP_END], BARMAN_TIME_FORMAT))
        if compare_timestamp(max_timestamp, temp) == temp:
            max_timestamp = temp
            backupid = backup[BARMAN_BACKUP_ID]

    return backupid


def get_oldest_backupid(backup_info: TypedDict) -> str:
    backupid = None
    min_timestamp = time.time()

    backup_info = backup_info[BARMAN_BACKUP_LISTS]
    for backup in backup_info:
        temp = time.mktime(
            time.strptime(backup[BARMAN_BACKUP_END], BARMAN_TIME_FORMAT))
        if compare_timestamp(min_timestamp, temp) == min_timestamp:
            min_timestamp = temp
            backupid = backup[BARMAN_BACKUP_ID]

    return backupid


def get_backup_name_env(meta: kopf.Meta, name: str = None) -> List:
    res = list()

    if name is None:
        name = meta['name']

    env = BARMAN_BACKUP_NAME + '="' + name + '"'
    res.append('-e')
    res.append(env)

    return res


def get_s3_env(s3: TypedDict) -> List:
    res = list()

    # use SPEC_S3 prefix replace S3 key (must use S3_ prefix)
    for k in list(s3.keys()):
        old = k
        new = SPEC_S3 + "_" + k
        s3[new] = s3.pop(old)

    for k, v in s3.items():
        env = k + '="' + v + '"'
        res.append('-e')
        res.append(env)

    return res


def get_policy_env(policy: TypedDict) -> List:
    res = list()

    default_policy = {
        SPEC_BACKUPTOS3_POLICY_ARCHIVE:
        SPEC_BACKUPTOS3_POLICY_ARCHIVE_DEFAULT_VALUE,
        SPEC_BACKUPTOS3_POLICY_COMPRESSION:
        SPEC_BACKUPTOS3_POLICY_COMPRESSION_DEFAULT_VALUE,
        SPEC_BACKUPTOS3_POLICY_ENCRYPTION:
        SPEC_BACKUPTOS3_POLICY_ENCRYPTION_DEFAULT_VALUE,
        SPEC_BACKUPTOS3_POLICY_RETENTION:
        SPEC_BACKUPTOS3_POLICY_RETENTION_DEFAULT_VALUE
    }

    for k, v in policy.items():
        env = k + '="' + v + '"'
        res.append('-e')
        res.append(env)

    for k, v in default_policy.items():
        if k not in policy:
            env = k + '="' + v + '"'
            res.append('-e')
            res.append(env)

    return res


def get_need_s3_env(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    need_envs: List = None,
) -> List:
    res = list()
    name = None

    if need_envs is None:
        need_envs = [SPEC_S3, SPEC_BACKUPTOS3_POLICY]

    if SPEC_S3 in need_envs:
        # add s3 env by pgtools
        s3 = copy.deepcopy(spec[SPEC_S3])
        res.extend(get_s3_env(s3))

    if SPEC_BACKUPTOS3_POLICY in need_envs:
        backup_policy = spec.get(SPEC_BACKUPCLUSTER,
                                 {}).get(SPEC_BACKUPTOS3,
                                         {}).get(SPEC_BACKUPTOS3_POLICY, {})
        res.extend(get_policy_env(backup_policy))

    if BACKUP_NAME in need_envs:
        name = spec.get(SPEC_BACKUPCLUSTER,
                        {}).get(SPEC_BACKUPTOS3,
                                {}).get(SPEC_BACKUPTOS3_NAME, None)
    elif RESTORE_NAME in need_envs:
        name = spec[RESTORE][RESTORE_FROMS3].get(RESTORE_FROMS3_NAME, None)
    res.extend(get_backup_name_env(meta, name))

    return res


def get_backup_status_from_backup_info(
        backup_info: TypedDict,
        need_field: List = BARMAN_STATUS_DEFAULT_NEED_FIELD) -> List:
    res = list()
    backup_info = backup_info[BARMAN_BACKUP_LISTS]
    for backup in backup_info:
        temp = dict()
        for field in need_field:
            if field in backup:
                temp[field] = backup[field]
        res.append(temp)
    return res


def get_s3_backup_list(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> (List, str):
    conns = pgsql_util.connections(
        spec, meta, patch, pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE),
        False, None, logger, None, status, False)
    s3_info = get_need_s3_env(meta, spec, patch, status, logger,
                              [SPEC_S3, SPEC_BACKUPTOS3_POLICY, BACKUP_NAME])

    cmd = ["pgtools", "-v"] + s3_info
    output = pgsql_util.exec_command(conns.get_conns()[0],
                                     cmd,
                                     logger,
                                     interrupt=True,
                                     user="postgres")
    if output == "":
        raise kopf.TemporaryError(
            "get_s3_backup_list get backup info failed, exit ...")
    elif output.find(MESSAGE_S3_CONNECT_FAILED) != -1:
        raise kopf.TemporaryError(
            "get_s3_backup_list connect s3 failed, exit ...")
    logger.warning(f"get_s3_backup_list get backup verbose info = {output}")

    backup_info = {BARMAN_BACKUP_LISTS: ""}
    try:
        backup_info = json.loads(output)
    except json.JSONDecodeError as e:
        logger.error(f"decode backup_info with error, {e}")

    latest_backup_id = get_latest_backupid(backup_info)

    return get_backup_status_from_backup_info(backup_info), latest_backup_id


def load_and_check_params(
    meta: kopf.Meta,
    spec: kopf.Spec,
    logger: logging.Logger,
    need_params: TypedDict,
) -> TypedDict:
    logger.info("load params.")
    res = dict()

    for k, v in need_params.items():
        if v is None:
            continue

        v_split = v.split('/')
        if len(v_split) > 1:
            temp = copy.deepcopy(spec)
            for i in range(0, len(v_split)):
                temp = temp.get(v_split[i], {})
            if temp:
                res[k] = temp
        elif len(v_split) == 1 and v is not None:
            res[k] = spec[v]

    res[METADATA_NAME] = meta[METADATA_NAME]
    res[METADATA_NAMESPACE] = meta[METADATA_NAMESPACE]

    if res[SPEC_BACKUP_BACKUPTOS3_KIND] == SPEC_BACKUP_BACKUPTOS3_KIND_SCHEDULE:
        if res.get(SPEC_BACKUP_BACKUPTOS3_SCHEDULE, None) is None:
            raise kopf.PermanentError(
                f"{SPEC_BACKUP_BACKUPTOS3_KIND_SCHEDULE} backup but {SPEC_BACKUP_BACKUPTOS3_SCHEDULE} is None"
            )
    elif res[
            SPEC_BACKUP_BACKUPTOS3_KIND] == SPEC_BACKUP_BACKUPTOS3_KIND_SIGNAL:
        res.pop(SPEC_BACKUP_BACKUPTOS3_SCHEDULE, None)
        logger.info(
            f"{SPEC_BACKUP_BACKUPTOS3_KIND_SIGNAL} backup will ignore {SPEC_BACKUP_BACKUPTOS3_SCHEDULE} config"
        )

    return res


def check_condition_pgsql_exists(
    logger: logging.Logger,
    backup_params: TypedDict,
) -> TypedDict:

    body = None
    customer_obj_api = client.CustomObjectsApi()
    name = backup_params[SPEC_CLUSTERNAME]
    namespace = backup_params[SPEC_CLUSTERNAMESPACE]

    try:
        # get customer definition
        body = customer_obj_api.get_namespaced_custom_object(
            group=API_GROUP,
            version=API_VERSION_V1,
            namespace=namespace,
            plural=RESOURCE_POSTGRESQL,
            name=name)
    except Exception as e:
        logger.error(f"error occurs, {e}")
        raise kopf.TemporaryError(
            f"check_condition_pgsql_exists can't get crd {RESOURCE_POSTGRESQL} with name {name}, namespace {namespace}, error {e}"
        )

    if body is None or not all(key in body for key in REQUIRED_KEYS):
        raise kopf.TemporaryError(
            f"check_condition_pgsql_exists get crd {RESOURCE_POSTGRESQL} with name {name}, namespace {namespace} failed."
        )
    return body


def delete_backup_cr(
    name: str,
    namespace: str,
    logger: logging.Logger,
) -> None:
    customer_obj_api = client.CustomObjectsApi()

    try:
        customer_obj_api.delete_namespaced_custom_object(
            group=API_GROUP,
            version=API_VERSION_V1,
            namespace=namespace,
            plural=RESOURCE_POSTGRESQL_BACKUP,
            name=name)
    except Exception as e:
        logger.error(f"error occurs, {e}")
        raise kopf.TemporaryError(
            f"delete_backup_cr delete {RESOURCE_POSTGRESQL_BACKUP} with name {name}, namespace {namespace} failed, error {e}"
        )


def check_condition_backup_param(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    backup_params: TypedDict,
) -> None:
    is_backup = False
    is_locked = False

    if is_s3_backup_mode(backup_params):
        is_backup = True
        check_s3 = check_condition_pgsql_s3_param(spec)
        is_locked = check_condition_pgsql_locked(meta, spec, patch, status,
                                                 logger)
        if not check_s3:
            is_backup = False

    if not is_backup:
        raise kopf.TemporaryError(
            f"S3 backup but crd {RESOURCE_POSTGRESQL} with name {backup_params[SPEC_CLUSTERNAME]}, namespace {backup_params[SPEC_CLUSTERNAMESPACE]} check failed, please check .spec.S3 field and check error log."
        )
    if is_locked:
        raise kopf.TemporaryError(
            f"cluster with name {backup_params[SPEC_CLUSTERNAME]}, namespace {backup_params[SPEC_CLUSTERNAMESPACE]} is backupLocked."
        )


def check_condition_pgsql_s3_param(spec: kopf.Spec, ) -> bool:
    if spec.get(SPEC_S3, None) is None:
        return False
    return True


def check_condition_pgsql_locked(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> bool:
    if status.get(STATE_BACK_LOCKING, None) != STATE_BACK_LOCKED:
        return False

    readwrite_conns = pgsql_util.connections(
        spec, meta, patch, pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE),
        False, None, logger, None, status, False)

    barman_check_cmd = ["ps -ef | grep -v grep | grep barman"]
    max_try = 10
    success_threshold = 3

    i = 0
    counter = 0
    for conn in readwrite_conns.get_conns():
        while True:
            i += 1
            time.sleep(1)

            output = pgsql_util.exec_command(conn,
                                             barman_check_cmd,
                                             logger,
                                             interrupt=False,
                                             user="postgres")
            if output != "":
                counter += 1
            else:
                counter = 0

            logger.info(
                f"check_condition_pgsql_locked check barman status try {i} times, output {output}"
            )
            if counter >= success_threshold:
                logger.warning(
                    f"check_condition_pgsql_locked find another barman process, exit backup."
                )
                return True

            if i > max_try:
                break

    logger.warning(f"maybe backup not end normally, reset the backup status")
    set_backup_status(meta,
                      STATE_BACK_LOCKING,
                      STATE_BACK_UNLOCKED,
                      logger,
                      plural=RESOURCE_POSTGRESQL)

    return False


def set_backup_status(
    meta: kopf.Meta,
    statefield: Any,
    state: Any,
    logger: logging.Logger,
    timeout: int = MINUTES,
    plural: str = RESOURCE_POSTGRESQL_BACKUP,
) -> None:
    pgsql_util.set_cluster_status(meta,
                                  statefield,
                                  state,
                                  logger,
                                  timeout=timeout,
                                  plural=plural)


def set_display_backup_status(
    meta: kopf.Meta,
    backup_status: TypedDict,
    logger: logging.Logger,
    timeout: int = MINUTES,
    plural: str = RESOURCE_POSTGRESQL_BACKUP,
    other_status: TypedDict = {},
) -> None:
    statefield = list()
    state = list()

    for key in BACKUP_STATUS_FIELD.keys():
        statefield.append(key)
        state.append(backup_status.get(BACKUP_STATUS_FIELD[key], ""))

    for key in other_status.keys():
        statefield.append(key)
        state.append(other_status.get(key, ""))

    set_backup_status(meta,
                      statefield,
                      state,
                      logger,
                      timeout=timeout,
                      plural=plural)


def is_s3_backup_mode(backup_params: TypedDict, ) -> bool:
    if backup_params.get(SPEC_BACKUP_BACKUPTOS3_KIND, None) is not None:
        return True

    return False


def generate_backup_alias_name(
    name: str,
    namespace: str,
) -> str:
    return name + FIELD_DELIMITER + namespace + FIELD_DELIMITER + str(
        time.time())


#########################################
# main cr
# #########################################
def get_backup_mode(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    except_backup_mode: List = None,
) -> str:
    if except_backup_mode is None:
        except_backup_mode = [BACKUP_MODE_S3_MANUAL, BACKUP_MODE_S3_CRON]

    if BACKUP_MODE_S3_MANUAL in except_backup_mode and is_s3_manual_backup_mode(
            meta, spec, patch, status, logger):
        return BACKUP_MODE_S3_MANUAL

    if BACKUP_MODE_S3_CRON in except_backup_mode and is_s3_cron_backup_mode(
            meta, spec, patch, status, logger):
        return BACKUP_MODE_S3_CRON

    return BACKUP_MODE_NONE


def is_backup_mode(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> bool:
    if spec.get(SPEC_BACKUPCLUSTER) == None:
        return False

    if spec[SPEC_BACKUPCLUSTER].get(SPEC_BACKUPTOS3) == None:
        return False

    return True


def is_s3_manual_backup_mode(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> bool:
    if is_backup_mode(meta, spec, patch, status, logger) == False:
        return False

    if spec.get(SPEC_S3) == None:
        return False

    if spec[SPEC_BACKUPCLUSTER].get(SPEC_BACKUPTOS3,
                                    {}).get(SPEC_BACKUPTOS3_MANUAL) != None:
        return True

    return False


def is_s3_cron_backup_mode(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> bool:
    if is_backup_mode(meta, spec, patch, status, logger) == False:
        return False

    if spec.get(SPEC_S3) == None:
        return False

    if spec[SPEC_BACKUPCLUSTER].get(SPEC_BACKUPTOS3,
                                    {}).get(SPEC_BACKUPTOS3_CRON) != None:
        return True

    return False
