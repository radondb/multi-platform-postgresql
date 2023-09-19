import logging
import kopf
import paramiko
import os
import copy
import string
import time
import random
import tempfile
import base64
import hashlib
import re

from kubernetes import client
from kubernetes.stream import stream

from pgsqlcommons.constants import *
from pgsqlcommons.typed import LabelType, InstanceConnection, InstanceConnections, TypedDict, InstanceConnectionMachine, InstanceConnectionK8S, Tuple, Any, List
from pgsqlcommons.config import operator_config
import pgsqlclusters.create as pgsql_create
import pgsqlclusters.update as pgsql_update

EXEC_COMMAND_DEFAULT_TIMEOUT = operator_config.BOOTSTRAP_TIMEOUT


def set_cluster_status(meta: kopf.Meta,
                       statefield: Any,
                       state: Any,
                       logger: logging.Logger,
                       timeout: int = MINUTES,
                       plural: str = RESOURCE_POSTGRESQL) -> None:
    customer_obj_api = client.CustomObjectsApi()
    name = meta['name']
    namespace = meta['namespace']

    # If only try to update once, you may get the object has been modified; please apply your changes to the latest version and try again error
    # see https://stackoverflow.com/questions/51297136/kubectl-error-the-object-has-been-modified-please-apply-your-changes-to-the-la get more infomations
    i = 0
    maxtry = timeout
    while True:
        i += 1
        time.sleep(SECONDS)
        if i >= maxtry:
            logger.error(f"set_cluster_status failed, skip.")
            break
        try:
            # get customer definition
            body = customer_obj_api.get_namespaced_custom_object(
                group=API_GROUP,
                version=API_VERSION_V1,
                namespace=namespace,
                plural=plural,
                name=name)
            state_dict = dict()
            if isinstance(statefield, list) and isinstance(state, list):
                if len(statefield) != len(state):
                    logger.error(f"Unknown Error.")
                    return
                for i in range(len(statefield)):
                    state_dict[statefield[i]] = state[i]
            else:
                state_dict[statefield] = state

            body.setdefault(CLUSTER_STATUS, {}).update(state_dict)

            customer_obj_api.patch_namespaced_custom_object(
                group=API_GROUP,
                version=API_VERSION_V1,
                namespace=namespace,
                plural=plural,
                name=name,
                body=body)
            logger.info(
                f"update {'%s.%s/%s' % (plural, API_GROUP, API_VERSION_V1)} crd {name} field .status.{statefield} = {state}, set_cluster_status body = {body}"
            )
            break
        except Exception as err:
            logger.warning(
                f"set_cluster_status failed, try {i} times. error: {err}")


def set_password(patch: kopf.Patch, status: kopf.Status) -> None:
    password_length = 8

    if status.get(AUTOCTL_NODE) == None:
        patch.status[AUTOCTL_NODE] = ''.join(
            random.sample(string.ascii_letters + string.digits,
                          password_length))

    if status.get(PGAUTOFAILOVER_REPLICATOR) == None:
        patch.status[PGAUTOFAILOVER_REPLICATOR] = ''.join(
            random.sample(string.ascii_letters + string.digits,
                          password_length))


def get_connhost(conn: InstanceConnection) -> str:

    if conn.get_k8s() != None:
        return pod_conn_get_pod_address(conn)
    if conn.get_machine() != None:
        return conn.get_machine().get_host()


def set_status_ssl_server_crt(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    conn: InstanceConnection,
) -> None:
    cmd = ["base64", PG_DATABASE_DIR + "/server.crt"]
    server_crt_base64 = exec_command(conn, cmd, logger, interrupt=False)
    set_cluster_status(meta, CLUSTER_STATUS_SERVER_CRT, server_crt_base64,
                       logger)


def create_ssl_key(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    conn: InstanceConnection = None,
) -> None:
    readwrite_conns = None
    readonly_conns = None
    conns = None

    if conn == None:
        waiting_cluster_final_status(meta, spec, patch, status, logger)
        readwrite_conns = connections(spec, meta, patch,
                                      get_field(POSTGRESQL, READWRITEINSTANCE),
                                      False, None, logger, None, status, False)
        readonly_conns = connections(spec, meta, patch,
                                     get_field(POSTGRESQL, READONLYINSTANCE),
                                     False, None, logger, None, status, False)
        conns = readwrite_conns.get_conns() + readonly_conns.get_conns()
        conn = conns[0]

    DNS = "DNS:"
    protect_dns = []
    autofailover_machines = spec.get(AUTOFAILOVER).get(MACHINES)
    # k8s mode
    if autofailover_machines == None:
        for service in spec[SERVICES]:
            protect_dns.append(DNS + get_service_name(meta, service))
    else:
        for service in spec[SERVICES]:
            protect_dns.append(DNS + service[VIP])

    cmd = [
        "openssl", "req", "-new", "-x509", "-days", "36500", "-nodes", "-text",
        "-out", PG_DATABASE_DIR + "/server.crt", "-keyout",
        PG_DATABASE_DIR + "/server.key",
        f'''-subj "/CN=www.qingcloud.com" -addext "subjectAltName = {', '.join(protect_dns)}"'''
    ]

    logger.info(f"create ssl key with cmd {cmd}")
    output = exec_command(conn, cmd, logger, interrupt=False, user="postgres")
    logger.info(f"create ssl key {output}")

    set_status_ssl_server_crt(meta, spec, patch, status, logger, conn)

    if conns != None and len(conns) > 1:
        cmd = ["base64", PG_DATABASE_DIR + "/server.crt"]
        server_crt_base64 = exec_command(conn, cmd, logger, interrupt=False)
        cmd = ["base64", PG_DATABASE_DIR + "/server.key"]
        server_key_base64 = exec_command(conn, cmd, logger, interrupt=False)
        for i in range(1, len(conns)):
            cmd = [
                "echo", server_crt_base64, "|", "base64", "-d", ">",
                PG_DATABASE_DIR + "/server.crt", "&&", "chown",
                "postgres:postgres", PG_DATABASE_DIR + "/server.crt"
            ]
            output = exec_command(conns[i],
                                  cmd,
                                  logger,
                                  interrupt=False,
                                  user="postgres")
            cmd = [
                "echo", server_key_base64, "|", "base64", "-d", ">",
                PG_DATABASE_DIR + "/server.key", "&&", "chown",
                "postgres:postgres", PG_DATABASE_DIR + "/server.key"
            ]
            output = exec_command(conns[i],
                                  cmd,
                                  logger,
                                  interrupt=False,
                                  user="postgres")

    if readwrite_conns != None:
        readwrite_conns.free_conns()
    if readonly_conns != None:
        readonly_conns.free_conns()


def autofailover_switchover(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    primary_host: str = None,
) -> None:
    auto_failover_conns = connections(spec, meta, patch,
                                      get_field(AUTOFAILOVER), False, None,
                                      logger, None, status, False)
    for conn in auto_failover_conns.get_conns():
        cmd = ["pgtools", "-o"]
        i = 0
        while True:
            logger.info(f"switchover with cmd {cmd}")
            if primary_host == None or primary_host == get_primary_host(
                    meta, spec, patch, status, logger):
                output = exec_command(conn, cmd, logger, interrupt=False)
                if output.find(SWITCHOVER_FAILED_MESSAGE) != -1:
                    logger.error(f"switchover failed, {output}")
                    i += 1
                    if i >= 100:
                        logger.error(f"switchover failed, skip..")
                        break
                    time.sleep(5)
                else:
                    waiting_cluster_final_status(meta, spec, patch, status,
                                                 logger)
                    break
            else:
                if primary_host != None or primary_host != get_primary_host(
                        meta, spec, patch, status, logger):
                    waiting_cluster_final_status(meta, spec, patch, status,
                                                 logger)
                break
    auto_failover_conns.free_conns()


def get_primary_host(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> str:
    auto_failover_conns = connections(spec, meta, patch,
                                      get_field(AUTOFAILOVER), False, None,
                                      logger, None, status, False)
    for conn in auto_failover_conns.get_conns():
        cmd = [
            "pgtools", "-w", "0", "-Q", "pg_auto_failover", "-q",
            '''" select nodehost from pgautofailover.node where reportedstate = 'primary' or reportedstate = 'wait_primary' or reportedstate = 'single'  "'''
        ]
        output = exec_command(conn, cmd, logger, interrupt=False)
        break
    auto_failover_conns.free_conns()
    return output.strip()


def waiting_cluster_final_status(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    timeout: int = MINUTES * 1,
    except_nodes: int = None,
) -> bool:
    is_health = True

    if spec[ACTION] == ACTION_STOP or in_disaster_backup(
            meta, spec, patch, status, logger) == True:
        return is_health

    # waiting for restart
    auto_failover_conns = connections(spec, meta, patch,
                                      get_field(AUTOFAILOVER), False, None,
                                      logger, None, status, False)
    for conn in auto_failover_conns.get_conns():
        not_correct_cmd = [
            "pgtools", "-w", "0", "-Q", "pg_auto_failover", "-q",
            f'''" select count(*) from pgautofailover.node where reportedstate <> 'primary' and reportedstate <> 'secondary' and reportedstate <> 'single' and nodename not like '{AUTOCTL_DISASTER_NAME}%' "'''
        ]
        primary_cmd = [
            "pgtools", "-w", "0", "-Q", "pg_auto_failover", "-q",
            f'''" select count(*) from pgautofailover.node where reportedstate = 'primary' or reportedstate = 'single' and nodename not like '{AUTOCTL_DISASTER_NAME}%' "'''
        ]
        nodes_cmd = [
            "pgtools", "-w", "0", "-Q", "pg_auto_failover", "-q",
            f'''" select count(*) from pgautofailover.node where nodename not like '{AUTOCTL_DISASTER_NAME}%' "'''
        ]

        i = 0
        maxtry = timeout
        while True:
            logger.info(
                f"waiting auto_failover cluster final status, {i} times. ")
            i += 1
            time.sleep(1)
            if i >= maxtry:
                logger.warning(
                    f"cluster maybe maybe not right. skip waitting.")
                is_health = False
                break
            output = exec_command(conn, primary_cmd, logger, interrupt=False)
            if output != '1':
                logger.warning(
                    f"not find primary node in autofailover, output is {output}"
                )
                continue
            output = exec_command(conn,
                                  not_correct_cmd,
                                  logger,
                                  interrupt=False)
            if output != '0':
                logger.warning(
                    f"there are {output} nodes is not primary/secondary/single"
                )
                continue

            if conn.get_machine() == None:
                total_nodes = int(
                    spec[POSTGRESQL][READWRITEINSTANCE][REPLICAS]) + int(
                        spec[POSTGRESQL][READONLYINSTANCE][REPLICAS])
            else:
                total_nodes = len(
                    spec.get(POSTGRESQL).get(READWRITEINSTANCE).get(MACHINES)
                ) + len(
                    spec.get(POSTGRESQL).get(READONLYINSTANCE).get(MACHINES))
            if except_nodes is not None:
                total_nodes = except_nodes
            output = exec_command(conn, nodes_cmd, logger, interrupt=False)
            if output != str(total_nodes):
                logger.warning(
                    f"there are {output} nodes in autofailover, expect {total_nodes} nodes"
                )
                continue

            break
    auto_failover_conns.free_conns()
    return is_health


def waiting_cluster_correct_status(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> None:
    if spec[ACTION] == ACTION_STOP:
        return

    auto_failover_conns = connections(spec, meta, patch,
                                      get_field(AUTOFAILOVER), False, None,
                                      logger, None, status, False)
    for conn in auto_failover_conns.get_conns():
        not_correct_cmd = [
            "pgtools", "-w", "0", "-Q", "pg_auto_failover", "-q",
            '''" select count(*) from pgautofailover.node where reportedstate <> 'primary' and reportedstate <> 'secondary' and reportedstate <> 'single' and reportedstate <> 'wait_primary' and reportedstate <> 'catchingup' and reportedstate <> 'wait_standby' "'''
        ]
        primary_cmd = [
            "pgtools", "-w", "0", "-Q", "pg_auto_failover", "-q",
            '''" select count(*) from pgautofailover.node where reportedstate = 'primary' or reportedstate ='wait_primary' or reportedstate = 'single'  "'''
        ]
        nodes_cmd = [
            "pgtools", "-w", "0", "-Q", "pg_auto_failover", "-q",
            '''" select count(*) from pgautofailover.node  "'''
        ]

        i = 0
        maxtry = 60
        while True:
            logger.info(
                f"waiting auto_failover correct cluster Status, {i} times. ")
            i += 1
            time.sleep(1)
            if i >= maxtry:
                logger.warning(
                    f"cluster maybe maybe not right. skip waitting.")
                break
            output = exec_command(conn, primary_cmd, logger, interrupt=False)
            if output != '1':
                logger.warning(
                    f"not find primary node in autofailover, output is {output}"
                )
                continue
            output = exec_command(conn,
                                  not_correct_cmd,
                                  logger,
                                  interrupt=False)
            if output != '0':
                logger.warning(
                    f"there are {output} nodes is not primary/secondary/single/wait_primary/catchingup"
                )
                continue

            if conn.get_machine() == None:
                total_nodes = int(
                    spec[POSTGRESQL][READWRITEINSTANCE][REPLICAS]) + int(
                        spec[POSTGRESQL][READONLYINSTANCE][REPLICAS])
            else:
                total_nodes = len(
                    spec.get(POSTGRESQL).get(READWRITEINSTANCE).get(MACHINES)
                ) + len(
                    spec.get(POSTGRESQL).get(READONLYINSTANCE).get(MACHINES))
            output = exec_command(conn, nodes_cmd, logger, interrupt=False)
            if output != str(total_nodes):
                logger.warning(
                    f"there are {output} nodes in autofailover, expect {total_nodes} nodes"
                )
                continue

            break
    auto_failover_conns.free_conns()


def in_disaster_backup(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> bool:
    if spec.get(SPEC_DISASTERBACKUP,
                {}).get(SPEC_DISASTERBACKUP_ENABLE) == True:
        return True


def waiting_postgresql_ready(
    conns: InstanceConnections,
    logger: logging.Logger,
    timeout: int = MINUTES * 5,
    connect_start: int = None,
    connect_end: int = None,
) -> bool:
    if connect_start is None:
        connect_start = 0
    if connect_end is None:
        connect_end = conns.get_number()
    conns = conns.get_conns()[connect_start:connect_end]

    for conn in conns:
        i = 0
        maxtry = timeout
        while True:
            output = exec_command(conn,
                                  WAITING_POSTGRESQL_READY_COMMAND,
                                  logger,
                                  interrupt=False)
            if output != INIT_FINISH_MESSAGE:
                i += 1
                time.sleep(1)
                logger.error(
                    f"postgresql {get_connhost(conn)} is not ready. try {i} times. {output}"
                )
                if i >= maxtry:
                    logger.warning(f"postgresql is not ready. skip waitting.")
                    return False
            else:
                break
    return True


def waiting_target_postgresql_ready(meta: kopf.Meta,
                                    spec: kopf.Spec,
                                    patch: kopf.Patch,
                                    field: str,
                                    status: kopf.Status,
                                    logger: logging.Logger,
                                    connect_start: int = None,
                                    connect_end: int = None,
                                    exit: bool = False,
                                    timeout: int = MINUTES * 5) -> None:
    conns: InstanceConnections = connections(spec, meta, patch, field, False,
                                             None, logger, None, status, False,
                                             None)
    status = waiting_postgresql_ready(conns, logger, timeout, connect_start,
                                      connect_end)
    conns.free_conns()
    if not status and exit:
        logger.error(
            f"waiting_postgresql_ready timeout, please check logs or events")
        raise kopf.PermanentError("waiting_postgresql_ready timeout.")


def waiting_instance_ready(conns: InstanceConnections,
                           logger: logging.Logger,
                           connect_start: int = None,
                           connect_end: int = None,
                           timeout: int = WAIT_TIMEOUT):
    if connect_start is None:
        connect_start = 0
    if connect_end is None:
        connect_end = conns.get_number()
    conns = conns.get_conns()[connect_start:connect_end]

    success_message = 'running success'
    cmd = ['echo', "'%s'" % success_message]
    for conn in conns:
        i = 0
        maxtry = timeout
        while True:
            output = exec_command(conn, cmd, logger, interrupt=False)
            if output != success_message:
                i += 1
                time.sleep(1)
                logger.warning(f"instance not start. try {i} times. {output}")
                if i >= maxtry:
                    logger.warning(f"instance not start. skip waitting.")
                    break
            else:
                break


def waiting_postgresql_recovery_completed(conns: InstanceConnections,
                                          logger: logging.Logger,
                                          timeout: int = WAIT_TIMEOUT) -> bool:

    recovery_is_success = False
    recover_completed_cmd = [
        "test", "-f",
        os.path.join(ASSIST_DIR, RECOVERY_FINISH), "||", "echo",
        FILE_NOT_EXISTS
    ]
    pg_running_cmd = [
        "head -1",
        os.path.join(PG_DATABASE_RESTORING_DIR, POSTMASTER_FILE), "|",
        "xargs kill -0"
    ]

    for conn in conns.get_conns():
        i = 0
        maxtry = timeout
        while True:
            i += 1
            time.sleep(1)
            output = exec_command(conn,
                                  recover_completed_cmd,
                                  logger,
                                  interrupt=False)
            if output.find(FILE_NOT_EXISTS) != -1:
                logger.warning(
                    f"recovery not completed. try {i} times. {output}")
            else:
                logger.info(f"recovery completed.")
                recovery_is_success = True
                break

            output = exec_command(conn,
                                  pg_running_cmd,
                                  logger,
                                  interrupt=False)
            if output.strip() != "":
                logger.warning(
                    f"waiting recovery but PostgreSQL is not running. maybe recovery not completed, please check PostgreSQL log. {output}"
                )
                break

            if i >= maxtry:
                logger.warning(f"recovery not completed. skip waitting.")
                break

    return recovery_is_success


# def waiting_target_postgresql_pg_basebackup_completed(
#         meta: kopf.Meta,
#         spec: kopf.Spec,
#         patch: kopf.Patch,
#         field: str,
#         status: kopf.Status,
#         logger: logging.Logger,
#         connect_start: int = None,
#         connect_end: int = None) -> None:
#     conns: InstanceConnections = connections(spec, meta, patch, field, False,
#                                              None, logger, None, status, False,
#                                              None)
#     waiting_pg_basebackup_completed(conns, logger, connect_start, connect_end)
#     conns.free_conns()


def waiting_pg_basebackup_completed(conns: InstanceConnections,
                                    logger: logging.Logger,
                                    connect_start: int = None,
                                    connect_end: int = None,
                                    timeout: int = DAYS) -> bool:
    if connect_start is None:
        connect_start = 0
    if connect_end is None:
        connect_end = conns.get_number()
    conns = conns.get_conns()[connect_start:connect_end]

    pg_basebackup_is_success = False
    pg_basebackup_precheck_cmd = [
        "test", "-d", PG_DATABASE_DIR, "&&", "ls", PG_DATABASE_DIR, "|", "wc",
        "-l", "||", "echo", "0"
    ]
    pg_basebackup_completed_cmd = [
        "ps -ef | grep -v grep | grep pg_basebackup"
    ]
    for conn in conns:
        i = 0
        maxtry = timeout
        pg_basebackup_precheck_timeout = 60
        success_counter = 0
        success_threshold = 10
        while True:
            i += 1
            time.sleep(1)

            output = exec_command(conn,
                                  pg_basebackup_precheck_cmd,
                                  logger,
                                  interrupt=False)
            if to_int(output) == 0:
                if i < pg_basebackup_precheck_timeout:
                    logger.warning(
                        f"pg_basebackup execute pg_basebackup_precheck_cmd for {get_connhost(conn)} not completed. try {i} times."
                    )
                    continue
                else:
                    logger.warning(
                        f"pg_basebackup execute pg_basebackup_precheck_cmd for {get_connhost(conn)} not completed. skip waitting."
                    )
                    break

            output = exec_command(conn,
                                  pg_basebackup_completed_cmd,
                                  logger,
                                  interrupt=False)
            if output.strip() == "":
                success_counter += 1
                logger.info(
                    f"pg_basebackup for {get_connhost(conn)} complete {success_counter} times, success_threshold is {success_threshold}."
                )
                if success_counter >= success_threshold:
                    pg_basebackup_is_success = True
                    break
            else:
                logger.warning(
                    f"pg_basebackup for {get_connhost(conn)} not completed. try {i} times."
                )
                success_counter = 0

            if i >= maxtry:
                logger.warning(
                    f"pg_basebackup for {get_connhost(conn)} not completed. skip waitting."
                )
                break

    return pg_basebackup_is_success


def get_replica_by_machine(
    spec: kopf.Spec,
    field: str,
    target_machine: str,
) -> int:
    mode, _, _, _ = get_replicas(spec)
    if mode != MACHINE_MODE:
        return None

    if len(field.split(FIELD_DELIMITER)) == 1:
        machines = spec.get(field).get(MACHINES)
    else:
        if len(field.split(FIELD_DELIMITER)) != 2:
            raise kopf.PermanentError(
                "error parse field, only support one '.'" + field)
        machines = spec.get(field.split(FIELD_DELIMITER)[0]).get(
            field.split(FIELD_DELIMITER)[1]).get(MACHINES)

    for i in range(len(machines)):
        if target_machine.strip() == machines[i].strip():
            return i

    return None


def get_realimage_from_env(yaml_image: str) -> str:
    image_list = yaml_image.split("/")
    res = list()
    # Assume res length is 3. (yaml image cannot support registry field)
    #  res[0] is registry
    #  res[1] is namespace
    #  res[2] is image and tag
    # if IMAGE_REGISTRY, NAMESPACE_OVERRIDE exists, replace registry, namespace
    for i in range(3 - len(image_list)):
        res.append("")
    res.extend(image_list)

    if operator_config.IMAGE_REGISTRY.strip():
        res[0] = operator_config.IMAGE_REGISTRY
    if operator_config.NAMESPACE_OVERRIDE.strip():
        res[1] = operator_config.NAMESPACE_OVERRIDE
    elif len(image_list) == 1:
        res[1] = "library"

    res = [i for i in res if i != ""]

    return '/'.join(res)


def get_antiaffinity(meta: kopf.Meta, labels: LabelType,
                     antiaffinity: TypedDict) -> List:
    podAntiAffinityTerm = antiaffinity[
        SPEC_ANTIAFFINITY_PODANTIAFFINITYTERM].strip()
    if not antiaffinity or podAntiAffinityTerm == "none" or labels[
            LABEL_SUBNODE] not in [
                value for value in podAntiAffinityTerm.split(FIELD_DELIMITER)
                if value != AUTOFAILOVER
            ]:
        return {}
    if antiaffinity[SPEC_ANTIAFFINITY_POLICY] == SPEC_ANTIAFFINITY_REQUIRED:
        antiaffinity[SPEC_ANTIAFFINITY_TOPOLOGYKEY] = "kubernetes.io/hostname"

    return get_antiaffinity_from_template(meta, antiaffinity)


def get_antiaffinity_from_template(meta: kopf.Meta,
                                   antiaffinity: TypedDict) -> List:
    podAntiAffinityTerm = antiaffinity[
        SPEC_ANTIAFFINITY_PODANTIAFFINITYTERM].strip()
    node = '-'.join(
        set(
            re.sub(LABEL_SUBNODE_READWRITE + "|" + LABEL_SUBNODE_READONLY,
                   LABEL_NODE_POSTGRESQL, podAntiAffinityTerm).split("-")))
    subnode = podAntiAffinityTerm

    labelSelector = {
        SPEC_ANTIAFFINITY_TOPOLOGYKEY:
        antiaffinity[SPEC_ANTIAFFINITY_TOPOLOGYKEY],
        "labelSelector": {
            "matchExpressions":
            get_antiaffinity_matchExpressions(
                get_antiaffinity_labels(meta, node, subnode))
        }
    }

    res = list()
    if SPEC_ANTIAFFINITY_REQUIRED in antiaffinity[SPEC_ANTIAFFINITY_POLICY]:
        res = [labelSelector]
    elif SPEC_ANTIAFFINITY_PREFERRED in antiaffinity[SPEC_ANTIAFFINITY_POLICY]:
        res = [{"weight": 100, "podAffinityTerm": labelSelector}]

    return res


def get_statefulset_name(name: str, field: str, replica: int) -> str:
    return name + "-" + field.split(FIELD_DELIMITER)[-1:][0] + "-" + str(
        replica)


def get_statefulset_service_name(name: str, field: str, replica: int) -> str:
    return get_statefulset_name(name, field, replica)


def statefulset_name_get_service_name(name: str) -> str:
    return name


def statefulset_name_get_external_service_name(name: str) -> str:
    return name + "-external"


def statefulset_name_get_pod_name(name: str) -> str:
    return name + "-0"


def pod_name_get_statefulset_name(name: str) -> str:
    return name[0:-2]  # erase "-0"


def get_pvc_name(pod_name: str):
    return POSTGRESQL_PVC_NAME + "-" + pod_name


def get_pod_name(name: str, field: str, replica: int) -> str:
    return get_statefulset_name(name, field, replica) + "-0"


def pod_conn_get_pod_address(conn: InstanceConnection) -> str:
    #op-pg-lzzhang-autofailover-0-0.op-pg-lzzhang-autofailover-0.default.svc.cluster.local
    #podname.svcname.namespace.svc.cluster.local
    return conn.get_k8s().get_podname() + "." + pod_name_get_statefulset_name(
        conn.get_k8s().get_podname()) + "." + conn.get_k8s().get_namespace(
        ) + ".svc.cluster.local"


def get_pod_address(name: str, field: str, replica: int,
                    namespace: str) -> str:
    #op-pg-lzzhang-autofailover-0-0.op-pg-lzzhang-autofailover-0.default.svc.cluster.local
    #podname.svcname.namespace.svc.cluster.local
    return get_pod_name(
        name, field, replica) + "." + get_statefulset_service_name(
            name, field, replica) + "." + namespace + ".svc.cluster.local"
    #return statefulset_name_get_external_service_name(get_statefulset_service_name(name, field, replica))


def get_exporter_env(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    field: str,
    container_exporter_need_copy: TypedDict,
) -> (str, List):
    container_exporter = copy.deepcopy(container_exporter_need_copy)
    if field == get_field(AUTOFAILOVER):
        port = AUTO_FAILOVER_PORT
        dbname = 'pg_auto_failover'
        query_path = '/etc/autofailover_queries.yaml'
    else:
        port = get_postgresql_config_port(meta, spec, patch, status, logger)
        dbname = 'postgres'
        query_path = '/etc/queries.yaml'

    data_source_name = f"user=postgres port={port} host=127.0.0.1 dbname={dbname} sslmode=disable"

    autofailover_machines = spec.get(AUTOFAILOVER).get(MACHINES)

    machine_exporter_env = ""
    k8s_exporter_env = []
    container_exporter_envs = container_exporter.get(CONTAINER_ENV)
    if container_exporter_envs != None:
        if autofailover_machines == None:
            k8s_exporter_env += container_exporter_envs
        else:
            for env in container_exporter_envs:
                name = env[CONTAINER_ENV_NAME]
                value = env[CONTAINER_ENV_VALUE]
                machine_exporter_env += f'{name}={value}\n'

    if autofailover_machines != None:
        machine_exporter_env += f'DATA_SOURCE_NAME={data_source_name}\n'
        machine_exporter_env += f'PG_EXPORTER_EXTEND_QUERY_PATH={query_path}\n'
    else:
        k8s_exporter_env.append({
            CONTAINER_ENV_NAME: "DATA_SOURCE_NAME",
            CONTAINER_ENV_VALUE: f'{data_source_name}'
        })
        k8s_exporter_env.append({
            CONTAINER_ENV_NAME: "PG_EXPORTER_EXTEND_QUERY_PATH",
            CONTAINER_ENV_VALUE: f'{query_path}'
        })

    return (machine_exporter_env, k8s_exporter_env)


def get_machine_exporter_env(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    field: str,
    container_exporter_need_copy: TypedDict,
) -> str:

    (machine_exporter_env, k8s_exporter_env) = get_exporter_env(
        meta, spec, patch, status, logger, field,
        copy.deepcopy(container_exporter_need_copy))
    return machine_exporter_env


def get_k8s_exporter_env(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    field: str,
    container_exporter_need_copy: TypedDict,
) -> List:
    (machine_exporter_env, k8s_exporter_env) = get_exporter_env(
        meta, spec, patch, status, logger, field,
        copy.deepcopy(container_exporter_need_copy))
    return k8s_exporter_env


def to_int(value: str, default: int = 0) -> int:
    try:
        return int(float(value))
    except:
        return default


def get_conn_role(conn: InstanceConnection) -> str:
    if conn.get_k8s() != None:
        return conn.get_k8s().get_role()
    if conn.get_machine() != None:
        return conn.get_machine().get_role()


def get_field_replicas(spec: kopf.Spec, field: str = None) -> int:

    mode, autofailover_replicas, readwrite_replicas, readonly_replicas = get_replicas(
        spec)

    if field is not None:
        replicas = 1
        if len(field.split(FIELD_DELIMITER)) == 1:
            replicas = autofailover_replicas
        else:
            if len(field.split(FIELD_DELIMITER)) != 2:
                raise kopf.PermanentError(
                    "error parse field, only support one '.'" + field)
            if field.split(FIELD_DELIMITER)[1] == READWRITEINSTANCE:
                replicas = readwrite_replicas
            elif field.split(FIELD_DELIMITER)[1] == READONLYINSTANCE:
                replicas = readonly_replicas
    return replicas


def get_replicas(spec: kopf.Spec) -> (str, int, int, int):
    mode = K8S_MODE
    autofailover_machines = spec.get(AUTOFAILOVER).get(MACHINES)
    # *_replicas will be replace in machine mode
    autofailover_replicas = 1
    readwrite_machines = spec.get(POSTGRESQL).get(READWRITEINSTANCE).get(
        MACHINES)
    readwrite_replicas = spec.get(POSTGRESQL).get(READWRITEINSTANCE).get(
        REPLICAS)
    readonly_machines = spec.get(POSTGRESQL).get(READONLYINSTANCE).get(
        MACHINES)
    readonly_replicas = spec.get(POSTGRESQL).get(READONLYINSTANCE).get(
        REPLICAS)
    if autofailover_machines != None or readwrite_machines != None or readonly_machines != None:
        mode = MACHINE_MODE
        autofailover_replicas = len(autofailover_machines)
        readwrite_replicas = len(readwrite_machines)
        readonly_replicas = len(readonly_machines)

        if autofailover_machines == None:
            raise kopf.PermanentError("autofailover machines not set")
        if readwrite_machines == None:
            raise kopf.PermanentError("readwrite machines not set")

    return mode, autofailover_replicas, readwrite_replicas, readonly_replicas


# Check parameters or get the number of field replicas
def check_param(meta: kopf.Meta,
                spec: kopf.Spec,
                patch: kopf.Patch,
                status: kopf.Status,
                logger: logging.Logger,
                create: bool = True) -> int:

    mode, autofailover_replicas, readwrite_replicas, readonly_replicas = get_replicas(
        spec)

    if mode == MACHINE_MODE:
        logger.info("running on machines mode")

        if autofailover_replicas != 1:
            raise kopf.PermanentError("autofailover only support one machine.")
        if readwrite_replicas < 1:
            raise kopf.PermanentError(
                "readwrite machines must set at lease one machine")

    if create and spec[ACTION] == ACTION_STOP:
        raise kopf.PermanentError("can't set stop at init cluster.")
    if readwrite_replicas < 1:
        raise kopf.PermanentError("readwrite replicas must set at lease one")
    if readonly_replicas < 0:
        raise kopf.PermanentError("readonly replicas must large than zero")
    if create and in_disaster_backup(meta, spec, patch, status,
                                     logger) == True:
        raise kopf.PermanentError("can't set disaster backup at create.")

    #maintenance_users = spec[POSTGRESQL][SPEC_POSTGRESQL_USERS].get(SPEC_POSTGRESQL_USERS_MAINTENANCE)
    #if maintenance_users == None or len(maintenance_users) == 0:
    #    raise kopf.PermanentError("at lease one maintenance user")

    logger.info("parameters are correct")


def update_pgpassfile(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> None:
    pgpassfile = ""

    #autoctl node
    autoctl_node_password = patch.status.get(AUTOCTL_NODE)
    if autoctl_node_password == None:
        autoctl_node_password = status.get(AUTOCTL_NODE)
    pgpassfile += pgsql_update.pgpassfile_item(AUTOCTL_NODE,
                                               autoctl_node_password)

    # PGAUTOFAILOVER_REPLICATOR
    autoctl_replicator_password = patch.status.get(PGAUTOFAILOVER_REPLICATOR)
    if autoctl_replicator_password == None:
        autoctl_replicator_password = status.get(PGAUTOFAILOVER_REPLICATOR)
    pgpassfile += pgsql_update.pgpassfile_item(PGAUTOFAILOVER_REPLICATOR,
                                               autoctl_replicator_password)

    # users
    pgpassfile += pgsql_update.get_pgpassfile(meta, spec, patch, status,
                                              logger,
                                              SPEC_POSTGRESQL_USERS_ADMIN)
    pgpassfile += pgsql_update.get_pgpassfile(
        meta, spec, patch, status, logger, SPEC_POSTGRESQL_USERS_MAINTENANCE)
    pgpassfile += pgsql_update.get_pgpassfile(meta, spec, patch, status,
                                              logger,
                                              SPEC_POSTGRESQL_USERS_NORMAL)

    logger.info(f"update pgpassfile, {pgpassfile}")
    conns = connections(spec, meta, patch,
                        get_field(POSTGRESQL, READWRITEINSTANCE), False, None,
                        logger, None, status, False)
    readonly_conns = connections(spec, meta, patch,
                                 get_field(POSTGRESQL, READONLYINSTANCE),
                                 False, None, logger, None, status, False)
    for conn in (conns.get_conns() + readonly_conns.get_conns()):
        # clean old data
        cmd = ["truncate", "--size", "0", PGPASSFILE_PATH]
        output = exec_command(conn, cmd, logger, interrupt=False)

        # sed can't work when file size is 0.
        cmd = ["truncate", "--size", "1", PGPASSFILE_PATH]
        output = exec_command(conn, cmd, logger, interrupt=False)

        # ">" can't run in docker exec
        for onepass in pgpassfile.split("\n"):
            if len(onepass) < 5:
                continue
            cmd = ["sed", "-i", "-e", "'1i" + onepass + "'", PGPASSFILE_PATH]
            output = exec_command(conn, cmd, logger, interrupt=False)

        cmd = ["chmod", "0600", PGPASSFILE_PATH]
        output = exec_command(conn, cmd, logger, interrupt=False)

        cmd = ["chown", "postgres:postgres", PGPASSFILE_PATH]
        output = exec_command(conn, cmd, logger, interrupt=False)
    conns.free_conns()
    readonly_conns.free_conns()


def get_autoctl_name(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> str:
    autofailover_conns = connections(spec, meta, patch,
                                     get_field(AUTOFAILOVER), False, None,
                                     logger, None, status, False)
    auto_failover_conn = autofailover_conns.get_conns()[0]
    if auto_failover_conn.get_machine() != None:
        hash_text = auto_failover_conn.get_machine().get_host()
    else:
        hash_text = get_pod_address(meta["name"], AUTOFAILOVER, 0,
                                    meta["namespace"])

    autofailover_conns.free_conns()

    return AUTOCTL_DISASTER_NAME + '_' + hashlib.md5(
        hash_text.encode()).hexdigest()[0:8]


def update_number_sync_standbys(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    is_delete: bool = False,
    force_disaster: bool = False,
) -> None:
    conns = None
    if in_disaster_backup(meta, spec, patch, status,
                          logger) == True or force_disaster == True:
        readwrite_conns = connections(spec, meta, patch,
                                      get_field(POSTGRESQL, READWRITEINSTANCE),
                                      False, None, logger, None, status, False)
        conns = readwrite_conns
    else:
        autofailover_conns = connections(spec, meta, patch,
                                         get_field(AUTOFAILOVER), False, None,
                                         logger, None, status, False)
        conns = autofailover_conns

    if in_disaster_backup(meta, spec, patch, status,
                          logger) == True or force_disaster == True:
        i = 0
        max_times = 60
        while True:
            # run select on source cluster autofailover node.
            number_sync_cmd = [
                "psql", "-t", "-c",
                f'''"select count(*) from pgautofailover.node where nodename <> '{get_autoctl_name(meta, spec,patch,status,logger)}' and replicationquorum = 't' "''',
                "-d",
                f'''postgres://autoctl_node:{spec[SPEC_DISASTERBACKUP][SPEC_DISASTERBACKUP_AUTOCTL_NODE]}@{spec[SPEC_DISASTERBACKUP][SPEC_DISASTERBACKUP_MONITOR_HOSTNAME]}:{AUTO_FAILOVER_PORT}/pg_auto_failover'''
            ]

            number_sync = exec_command(conns.get_conns()[0],
                                       number_sync_cmd,
                                       logger,
                                       interrupt=False)
            try:
                int(number_sync)
            except:
                number_sync = FAILED

            if number_sync == FAILED:
                logger.warning(
                    "can't select node info from source disaster cluster")
                time.sleep(SECONDS)
                i += 1
                if i >= max_times:
                    conns.free_conns()
                    logger.warning(
                        "can't select node info from source disaster cluster, skip update_number_sync_standbys"
                    )
                    return
            else:
                break

        number_sync = int(number_sync)
        if is_delete == False:
            number_sync = number_sync + 1 if spec[SPEC_DISASTERBACKUP][
                SPEC_DISASTERBACKUP_STREAMING] == STREAMING_SYNC else number_sync
        logger.info(
            f"there are {number_sync} sync nodes in the source cluster on disaster mode."
        )
    else:
        mode, autofailover_replicas, readwrite_replicas, readonly_replicas = get_replicas(
            spec)

        # local cluster sync number
        number_sync = readwrite_replicas + readonly_replicas if spec[
            POSTGRESQL][READONLYINSTANCE][
                STREAMING] == STREAMING_SYNC else readwrite_replicas

        # other cluster disaster sync number
        disaster_sync_nodes_cmd = [
            "pgtools", "-w", "0", "-Q", "pg_auto_failover", "-q",
            f'''" select count(*) from pgautofailover.node where nodename like '{AUTOCTL_DISASTER_NAME}%' and replicationquorum = 't' "'''
        ]
        disaster_sync_nodes = exec_command(conns.get_conns()[0],
                                           disaster_sync_nodes_cmd,
                                           logger,
                                           interrupt=False)
        number_sync += int(disaster_sync_nodes)
        logger.info(f"there are {number_sync} sync nodes in the cluster")

    # except sync number
    expect_number = number_sync - 2
    if expect_number < 0:
        expect_number = 0

    cmd = [
        "pgtools", "-S", "' formation number-sync-standbys  " +
        str(expect_number) + PRIMARY_FORMATION + "'"
    ]
    i = 0
    while True:
        logger.info(f"set number-sync-standbys with cmd {cmd}")
        output = exec_command(conns.get_conns()[0],
                              cmd,
                              logger,
                              interrupt=False)
        if output.find(SUCCESS) == -1:
            logger.error(f"set number-sync-standbys failed {cmd}  {output}")
            i += 1
            if i >= 60:
                logger.error(f"set number-sync-standbys failed, skip ")
                break
        else:
            break
    conns.free_conns()


def connect_pods(
    meta: kopf.Meta,
    spec: kopf.Spec,
    field: str,
) -> InstanceConnections:

    conns: InstanceConnections = InstanceConnections()

    replicas = get_field_replicas(spec, field)
    role = AUTOFAILOVER if len(
        field.split(FIELD_DELIMITER)) == 1 else POSTGRESQL

    for replica in range(0, replicas):
        name = get_pod_name(meta["name"], field, replica)
        namespace = meta["namespace"]
        conn = InstanceConnection(None,
                                  InstanceConnectionK8S(name, namespace, role))
        conns.add(conn)

    return conns


def connect_machine(machine: str,
                    role: str = POSTGRESQL) -> InstanceConnection:
    username = machine.split(":")[0]
    password = machine.split(":")[1]
    host = machine.split(":")[2]
    port = int(machine.split(":")[3])

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    i = 0
    max_times = 600

    while True:
        try:
            ssh.connect(host, username=username, port=port, password=password)
            time.sleep(0.1)
            break
        except Exception as e:
            time.sleep(1)
            i += 1
            if i >= max_times:
                raise kopf.PermanentError(
                    f"ssh can't connect to machine {machine} : {e}")
                break

    trans = paramiko.Transport((host, port))
    try:
        trans.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(trans)
    except Exception as e:
        raise kopf.PermanentError(
            f"sftp can't connect to machine {machine} : {e}")

    conn = InstanceConnection(
        InstanceConnectionMachine(host, port, username, password, ssh, sftp,
                                  trans, role), None)
    cmd = "mkdir -p " + os.path.join(operator_config.DATA_PATH_AUTOFAILOVER,
                                     DOCKER_COMPOSE_DIR)
    machine_exec_command(conn.get_machine().get_ssh(), cmd)
    cmd = "mkdir -p " + os.path.join(operator_config.DATA_PATH_POSTGRESQL,
                                     DOCKER_COMPOSE_DIR)
    machine_exec_command(conn.get_machine().get_ssh(), cmd)
    return conn


def machine_sftp_put(sftp: paramiko.SFTPClient, buffer: str,
                     remotepath: str) -> None:
    try:
        tf = tempfile.NamedTemporaryFile()
        tf.write(buffer.encode('utf-8'))
        tf.flush()
        sftp.put(localpath=tf.name, remotepath=remotepath)
        tf.close()
    except Exception as e:
        raise kopf.PermanentError(
            f"can't put file to remote {remotepath} : {e}")


def machine_sftp_get(sftp: paramiko.SFTPClient, localpath: str,
                     remotepath: str) -> None:
    try:
        sftp.get(remotepath=remotepath, localpath=localpath)
    except Exception as e:
        raise kopf.PermanentError(
            f"can't get file from remote {remotepath} : {e}")


def multi_exec_command(
    conns: InstanceConnections,
    cmds: List,
    logger: logging.Logger,
    interrupt: bool = True,
    user: str = "root",
) -> None:
    for conn in conns.get_conns():
        for cmd in cmds:
            exec_command(conn, cmd, logger, interrupt=interrupt, user=user)


def exec_command(conn: InstanceConnection,
                 cmd: [str],
                 logger: logging.Logger,
                 interrupt: bool = True,
                 user: str = "root",
                 timeout: int = EXEC_COMMAND_DEFAULT_TIMEOUT):
    ret = None
    if conn.get_k8s() != None:
        ret = pod_exec_command(conn.get_k8s().get_podname(),
                               conn.get_k8s().get_namespace(), cmd, logger,
                               interrupt, user, timeout)
    if conn.get_machine() != None:
        ret = docker_exec_command(conn.get_machine().get_role(),
                                  conn.get_machine().get_ssh(), cmd, logger,
                                  interrupt, user,
                                  conn.get_machine().get_host(), timeout)
    if ret == None:
        ret = ''

    return ret


def pod_exec_command(name: str,
                     namespace: str,
                     cmd: [str],
                     logger: logging.Logger,
                     interrupt: bool = True,
                     user: str = "root",
                     timeout: int = EXEC_COMMAND_DEFAULT_TIMEOUT) -> str:
    try:
        core_v1_api = client.CoreV1Api()
        # stderr stdout all in resp. don't have return code.
        resp = stream(
            core_v1_api.connect_get_namespaced_pod_exec,
            name,
            namespace,
            command=["/bin/bash", "-c", " ".join(['gosu', user] + cmd)],
            stderr=True,
            container=PODSPEC_CONTAINERS_POSTGRESQL_CONTAINER,
            stdin=False,
            stdout=True,
            tty=False,
            _preload_content=False)
        # in order to keep json format.
        # more information please visit https://github.com/kubernetes-client/python/issues/811#issuecomment-663458763
        resp.run_forever(timeout=timeout)
        return resp.read_stdout().replace(
            '\n', '') + resp.read_stderr().replace('\n', '')
    except Exception as e:
        if interrupt:
            raise kopf.PermanentError(
                f"pod {name} exec command({cmd}) failed {e}")
        else:
            logger.error(f"pod {name} exec command({cmd}) failed {e}")
            return FAILED


def string_to_base64(cmd: str) -> str:
    return base64.b64encode(cmd.encode("utf-8")).decode()


def docker_exec_command(role: str,
                        ssh: paramiko.SSHClient,
                        cmd: [str],
                        logger: logging.Logger,
                        interrupt: bool = True,
                        user: str = "root",
                        host: str = None,
                        timeout: int = EXEC_COMMAND_DEFAULT_TIMEOUT) -> str:
    if role == AUTOFAILOVER:
        machine_data_path = operator_config.DATA_PATH_AUTOFAILOVER
    if role == POSTGRESQL:
        machine_data_path = operator_config.DATA_PATH_POSTGRESQL
    try:
        workdir = os.path.join(machine_data_path, DOCKER_COMPOSE_DIR)
        #cmd = "cd " + workdir + "; docker-compose exec " + role + " " + " ".join(cmd)
        base64_cmd = [string_to_base64(" ".join(cmd))]
        user_cmd = "docker exec " + role + " " + " ".join(
            ['gosu', user, 'pgtools', '-f'] + base64_cmd)
        logger.info(f"docker exec command {cmd} on host {host}"
                    )  # not user_cmd, it is base64_cmd
        ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(user_cmd,
                                                             timeout=timeout,
                                                             get_pty=True)
    except Exception as e:
        if interrupt:
            raise kopf.PermanentError(f"can't run command: {cmd} , {e}")
        else:
            logger.error(f"can't run command: {cmd} , {e}")
            return FAILED

    # see pod_exec_command, don't check ret_code
    std_output = ssh_stdout.read().decode().strip()
    if std_output == None:
        std_output = ''
    err_output = ssh_stderr.read().decode().strip()
    if err_output == None:
        err_output = ''
    #ret_code = ssh_stdout.channel.recv_exit_status()
    #if ret_code != 0:
    #    if interrupt:
    #        raise kopf.PermanentError(
    #            f"docker {ssh} exec command {cmd}  failed, resp is: {err_output} {std_output}"
    #        )
    #    else:
    #        logger.error(
    #            f"docker {ssh} exec command {cmd}  failed, resp is: {err_output} {std_output}"
    #        )
    #        return FAILED

    #return str(ssh_stdout.read()).replace('\\r\\n', '').replace('\\n', '')[2:-1]
    ret = std_output + err_output
    return ret.replace('\r\n', '\n')


def machine_exec_command(ssh: paramiko.SSHClient,
                         cmd: str,
                         interrupt: bool = True) -> str:
    try:
        ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
    except Exception as e:
        raise kopf.PermanentError(f"can't run command: {cmd} , error {e}")

    err_output = ssh_stderr.read().decode()
    ret_code = ssh_stdout.channel.recv_exit_status()
    if ret_code != 0:
        if interrupt:
            raise kopf.TemporaryError(
                f"can't run command: {cmd} , error {err_output}")

    return ssh_stdout.read().decode().strip() + err_output


def connections(
    spec: kopf.Spec,
    meta: kopf.Meta,
    patch: kopf.Patch,
    field: str,
    create: bool,
    labels: LabelType,
    logger: logging.Logger,
    create_begin: int,
    status: kopf.Status,
    wait_primary: bool,
    create_end: int = None,
) -> InstanceConnections:
    origin_field = field
    if field == get_field(POSTGRESQL, DISASTER):
        field = get_field(POSTGRESQL, READWRITEINSTANCE)

    conns: InstanceConnections = InstanceConnections()
    if len(field.split(FIELD_DELIMITER)) == 1:
        machines = spec.get(AUTOFAILOVER).get(MACHINES)
        role = AUTOFAILOVER
    else:
        if len(field.split(FIELD_DELIMITER)) != 2:
            raise kopf.PermanentError(
                "error parse field, only support one '-'" + field)
        machines = spec.get(field.split(FIELD_DELIMITER)[0]).get(
            field.split(FIELD_DELIMITER)[1]).get(MACHINES)
        role = POSTGRESQL

    if machines == None:
        #logger.info("connect node in k8s mode")
        conns = connect_pods(meta, spec, field)
        if create:
            pgsql_create.create_postgresql(K8S_MODE, spec, meta, origin_field,
                                           labels, logger, conns, patch,
                                           create_begin, status, wait_primary,
                                           create_end)
    else:
        for replica, machine in enumerate(machines):
            conn = connect_machine(machine, role)
            #logger.info("connect node in machine mode, host is " + conn.get_machine().get_host())
            conns.add(conn)
        if create:
            pgsql_create.create_postgresql(MACHINE_MODE, spec, meta,
                                           origin_field, labels, logger, conns,
                                           patch, create_begin, status,
                                           wait_primary, create_end)
    return conns


def get_field(*fields):
    field = ""

    for i, f in enumerate(fields):
        if i > 0:
            field += FIELD_DELIMITER + f
        else:
            field = f

    return field


def get_base_labels(meta: kopf.Meta) -> TypedDict:
    base_labels = {
        BASE_LABEL_MANAGED_BY: POSTGRES_OPERATOR,
        BASE_LABEL_PART_OF: RADONDB_POSTGRES,
        BASE_LABEL_NAME: meta["name"],
        BASE_LABEL_NAMESPACE: meta["namespace"],
    }
    return base_labels


def get_antiaffinity_labels(meta: kopf.Meta, node: str,
                            subnode: str) -> TypedDict:
    labels = get_base_labels(meta)
    labels[LABEL_NODE] = node
    labels[LABEL_SUBNODE] = subnode
    return labels


def get_antiaffinity_matchExpressions(expressions: TypedDict) -> TypedDict:
    if not expressions:
        return {}
    antiaffinity_matchExpressions = list()
    for key in expressions:
        temp = {
            'key':
            key,
            'operator':
            'In',
            'values':
            expressions[key].split(FIELD_DELIMITER)
            if key in [LABEL_NODE, LABEL_SUBNODE] else [expressions[key]]
        }
        antiaffinity_matchExpressions.append(temp)

    return antiaffinity_matchExpressions


def get_service_name(meta: kopf.Meta, service: TypedDict) -> str:
    return meta["name"] + "-" + service["metadata"]["name"]


def get_service_autofailover_labels(meta: kopf.Meta) -> TypedDict:
    labels = get_base_labels(meta)
    labels[LABEL_NODE] = LABEL_NODE_AUTOFAILOVER
    labels[LABEL_SUBNODE] = LABEL_SUBNODE_AUTOFAILOVER
    return labels


def get_service_primary_labels(meta: kopf.Meta) -> TypedDict:
    labels = get_base_labels(meta)
    labels[LABEL_NODE] = LABEL_NODE_POSTGRESQL
    labels[LABEL_SUBNODE] = LABEL_SUBNODE_READWRITE
    labels[LABEL_ROLE] = LABEL_ROLE_PRIMARY
    return labels


def dict_to_str(dicts: TypedDict) -> str:
    r = ""
    i = 0
    for d in dicts:
        i += 1
        if i > 1:
            r += ", "
        r += d + "=" + dicts[d]
    return r


def get_readwrite_labels(meta: kopf.Meta) -> TypedDict:
    labels = get_base_labels(meta)
    labels[LABEL_NODE] = LABEL_NODE_POSTGRESQL
    labels[LABEL_SUBNODE] = LABEL_SUBNODE_READWRITE
    return labels


def get_autofailover_labels(meta: kopf.Meta) -> TypedDict:
    labels = get_base_labels(meta)
    labels[LABEL_NODE] = LABEL_NODE_AUTOFAILOVER
    labels[LABEL_SUBNODE] = LABEL_SUBNODE_AUTOFAILOVER
    return labels


def get_readonly_labels(meta: kopf.Meta) -> TypedDict:
    labels = get_base_labels(meta)
    labels[LABEL_NODE] = LABEL_NODE_POSTGRESQL
    labels[LABEL_SUBNODE] = LABEL_SUBNODE_READONLY
    labels[LABEL_ROLE] = LABEL_ROLE_STANDBY
    return labels


def get_service_standby_labels(meta: kopf.Meta) -> TypedDict:
    labels = get_base_labels(meta)
    labels[LABEL_NODE] = LABEL_NODE_POSTGRESQL
    labels[LABEL_SUBNODE] = LABEL_SUBNODE_READWRITE
    labels[LABEL_ROLE] = LABEL_ROLE_STANDBY
    return labels


def get_service_readonly_labels(meta: kopf.Meta) -> TypedDict:
    labels = get_base_labels(meta)
    labels[LABEL_NODE] = LABEL_NODE_POSTGRESQL
    labels[LABEL_SUBNODE] = LABEL_SUBNODE_READONLY
    labels[LABEL_ROLE] = LABEL_ROLE_STANDBY
    return labels


def get_service_standby_readonly_labels(meta: kopf.Meta) -> TypedDict:
    labels = get_base_labels(meta)
    labels[LABEL_NODE] = LABEL_NODE_POSTGRESQL
    labels[LABEL_ROLE] = LABEL_ROLE_STANDBY
    return labels


def get_primary_conn(conns: InstanceConnections,
                     timeout: int,
                     logger: logging.Logger,
                     interrupt: bool = True) -> InstanceConnection:
    if timeout <= 1:
        timeout = 1

    for i in range(0, timeout):
        for conn in conns.get_conns():
            cmd = ["pgtools", "-w", "0", "-q", "'show transaction_read_only'"]
            output = exec_command(conn, cmd, logger, interrupt=False)
            if output == "off":
                return conn
        time.sleep(1)
        logger.warning(f"get primary conn failed. try again {i} times.")

    if interrupt:
        raise kopf.TemporaryError("can't find postgresql primary node")
    else:
        return None


def connections_target(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    field: str,
    target_machines: List,
    target_k8s: List,
) -> InstanceConnections:

    conns: InstanceConnections = InstanceConnections()
    if len(field.split(FIELD_DELIMITER)) == 1:
        role = AUTOFAILOVER
    else:
        role = POSTGRESQL

    if target_machines != None:
        for replica, machine in enumerate(target_machines):
            conn = connect_machine(machine, role)
            logger.info("connect node in machine mode, host is " +
                        conn.get_machine().get_host())
            conns.add(conn)
    if target_k8s != None:
        for replica in range(target_k8s[0], target_k8s[1]):
            name = get_pod_name(meta["name"], field, replica)
            namespace = meta["namespace"]
            conn = InstanceConnection(
                None, InstanceConnectionK8S(name, namespace, role))
            logger.info("connect node in k8s mode, podname is " + name)
            conns.add(conn)

    return conns


def get_statefulset_service_labels(meta: kopf.Meta) -> str:
    labels = get_base_labels(meta)
    labels[LABEL_NODE] = LABEL_NODE_STATEFULSET_SERVICES

    return labels


def get_service_labels(meta: kopf.Meta) -> str:
    labels = get_base_labels(meta)
    labels[LABEL_NODE] = LABEL_NODE_USER_SERVICES

    return labels


def get_postgresql_config_port(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> int:
    configs = spec[POSTGRESQL][CONFIGS]
    for i, config in enumerate(configs):
        name = config.split("=")[0].strip()
        value = config[config.find("=") + 1:].strip()
        if name == 'port':
            return int(value)

    return 5432


def patch_statefulset_replicas(replicas: int) -> TypedDict:
    return {"spec": {"replicas": replicas}}


def patch_statefulset_restartPolicy(policy: str) -> TypedDict:
    # Always Never
    return {"spec": {"template": {"spec": {"restartPolicy": policy}}}}


def patch_role_body(role: str) -> TypedDict:
    role_body = {"metadata": {"labels": {"role": role}}}
    return role_body


def patch_pvc_body(size: str) -> TypedDict:
    pvc_body = {"spec": {"resources": {"requests": {"storage": size}}}}
    return pvc_body


#  name: data-zzz-postgresql-readwriteinstance-0-0
#spec:
#  resources:
#    requests:
#      storage: 10Gi
def resize_pvc(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    pvc_name: str,
    size: str,
) -> None:
    core_v1_api = client.CoreV1Api()

    try:
        real_size, real_status = read_pvc_size_and_status(
            meta, spec, patch, status, logger, pvc_name)
        core_v1_api.patch_namespaced_persistent_volume_claim(
            pvc_name, meta["namespace"], patch_pvc_body(size))

        if convert_to_bytes(real_size) >= convert_to_bytes(size):
            logger.warning(f"pvc {pvc_name} does not need expand.")
            return

        i = 0
        while i < WAIT_TIMEOUT and real_status != "FileSystemResizePending" and convert_to_bytes(
                real_size) < convert_to_bytes(size):
            real_size, real_status = read_pvc_size_and_status(
                meta, spec, patch, status, logger, pvc_name)
            i += 1
            time.sleep(SECONDS)
            logger.warning(
                f"resize_pvc on {pvc_name} not success, try {i} times. current status is {real_status}, current size is {real_size}"
            )
            if i == WAIT_TIMEOUT:
                logger.error(
                    f"resize_pvc on {pvc_name} timeout, skip waiting. current status is {real_status}, current size is {real_size}"
                )
    except Exception as e:
        logger.error(
            "Exception when calling AppsV1Api->patch_namespaced_persistent_volume_claim or read_namespaced_persistent_volume_claim: %s\n"
            % e)
        time.sleep(SECONDS * 10)


def read_pvc_size_and_status(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    pvc_name: str,
) -> (str, str):
    core_v1_api = client.CoreV1Api()

    pvc = client.V1PersistentVolumeClaim(
        core_v1_api.read_namespaced_persistent_volume_claim(
            name=pvc_name, namespace=meta["namespace"]))
    pvc_status = pvc.to_dict().get("api_version", {}).get("status", {})
    real_size = pvc_status.get("capacity", {}).get("storage")
    real_status = ""
    if pvc_status.get("conditions", None) is not None:
        real_status = pvc_status.get("conditions", [])[0].get("type")
    return real_size, real_status


def convert_to_bytes(pvc: str) -> int:
    index = 0
    for i in range(len(pvc)):
        try:
            int(pvc[i])
        except:
            index = i
            break
    value = pvc[:index]
    unit = pvc[index:]
    return int(value) * int(units.get(unit, 1))


def postgresql_action(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    conn: InstanceConnection,
    start: bool,
) -> None:
    if start == False:
        cmd = ["pgtools", "-R"]
        logger.info(f"stop postgresql with cmd {cmd} ")
        output = exec_command(conn, cmd, logger, interrupt=False)
        if output.find(STOP_FAILED_MESSAGE) != -1:
            logger.warning(f"can't stop postgresql. {output}, force stop it")
    if conn.get_k8s() != None:
        if start:
            replicas = STATEFULSET_REPLICAS
        else:
            replicas = 0

        try:
            apps_v1_api = client.AppsV1Api()

            name = pod_name_get_statefulset_name(conn.get_k8s().get_podname())
            namespace = conn.get_k8s().get_namespace()

            logger.info("set statefulset " + name + " replicas to " +
                        str(replicas))
            apps_v1_api.patch_namespaced_stateful_set(
                name, namespace, patch_statefulset_replicas(replicas))
        except Exception as e:
            logger.error(
                "Exception when calling AppsV1Api->patch_namespaced_stateful_set: %s\n"
                % e)
    if conn.get_machine() != None:
        if start:
            #cmd = "start"
            cmd = "up -d"
        else:
            #cmd = "stop"
            cmd = "down"

        logger.info("host " + conn.get_machine().get_host() + " postgresql " +
                    cmd)
        if conn.get_machine().get_role() == AUTOFAILOVER:
            machine_data_path = operator_config.DATA_PATH_AUTOFAILOVER
        if conn.get_machine().get_role() == POSTGRESQL:
            machine_data_path = operator_config.DATA_PATH_POSTGRESQL
        machine_exec_command(
            conn.get_machine().get_ssh(),
            "cd " + os.path.join(machine_data_path, DOCKER_COMPOSE_DIR) +
            "; docker-compose " + cmd)
