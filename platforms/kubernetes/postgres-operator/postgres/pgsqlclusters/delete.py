import kopf
import logging

from kubernetes import client

from pgsqlcommons.typed import LabelType, InstanceConnection, InstanceConnections, TypedDict, InstanceConnectionMachine, InstanceConnectionK8S, Tuple, Any, List
from pgsqlcommons.constants import *
from pgsqlcommons.config import operator_config
import pgsqlclusters.create as pgsql_create
import pgsqlclusters.utiles as pgsql_util
import pgsqlbackups.backup as pgsql_backup


# only stop the instance not do pgtools -d/-D
def delete_postgresql(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    delete_disk: bool,
    conn: InstanceConnection,
) -> None:
    grace_period_seconds = 70

    if delete_disk == True:
        logger.info("delete postgresql instance from autofailover")
        if pgsql_util.get_primary_host(
                meta, spec, patch, status,
                logger) == pgsql_util.get_connhost(conn):
            pgsql_util.autofailover_switchover(
                meta,
                spec,
                patch,
                status,
                logger,
                primary_host=pgsql_util.get_connhost(conn))
        if pgsql_util.get_conn_role(conn) == POSTGRESQL:
            cmd = ["pgtools", "-D"]
            output = pgsql_util.exec_command(conn,
                                             cmd,
                                             logger,
                                             interrupt=False)
            if output.find("drop auto_failover failed") != -1:
                logger.error("can't delete postgresql instance " + output)
    else:
        cmd = ["pgtools", "-R"]
        logger.info(f"stop postgresql with cmd {cmd} ")
        output = pgsql_util.exec_command(conn, cmd, logger, interrupt=False)
        if output.find(STOP_FAILED_MESSAGE) != -1:
            logger.warning(f"can't stop postgresql. {output}, force stop it")

    if conn.get_machine() != None:
        pgsql_create.machine_postgresql_down(conn, logger)
    elif conn.get_k8s() != None:
        try:
            apps_v1_api = client.AppsV1Api()
            logger.info("delete postgresql instance statefulset from k8s " +
                        pgsql_util.pod_name_get_statefulset_name(
                            conn.get_k8s().get_podname()))
            api_response = apps_v1_api.delete_namespaced_stateful_set(
                pgsql_util.pod_name_get_statefulset_name(
                    conn.get_k8s().get_podname()),
                conn.get_k8s().get_namespace(),
                grace_period_seconds=grace_period_seconds)
        except Exception as e:
            logger.error(
                f"Exception when calling AppsV1Api->delete_namespaced_stateful_set: {e} "
            )
        # if Node is shutdown, delete statefulset cannot delete pod, the pod will be in the Terminating state for a long time. so need delete pod.
        # More infomation please visit: https://kubernetes.io/zh-cn/docs/tasks/run-application/force-delete-stateful-set-pod/
        try:
            core_v1_api = client.CoreV1Api()
            # delete_pod override grace_period_seconds param
            delete_pod_grace_period_seconds = 0
            logger.info("delete postgresql pod from k8s: " +
                        conn.get_k8s().get_podname())
            core_v1_api.delete_namespaced_pod(
                conn.get_k8s().get_podname(),
                conn.get_k8s().get_namespace(),
                grace_period_seconds=delete_pod_grace_period_seconds)
        except Exception as e:
            logger.warning(
                "Exception when calling CoreV1Api->delete_namespaced_pod: %s\n"
                % e)
        try:
            core_v1_api = client.CoreV1Api()
            logger.info("delete postgresql instance service from k8s " +
                        pgsql_util.statefulset_name_get_service_name(
                            pgsql_util.pod_name_get_statefulset_name(
                                conn.get_k8s().get_podname())))
            delete_response = core_v1_api.delete_namespaced_service(
                pgsql_util.statefulset_name_get_service_name(
                    pgsql_util.pod_name_get_statefulset_name(
                        conn.get_k8s().get_podname())),
                conn.get_k8s().get_namespace())
        except Exception as e:
            logger.error(
                f"Exception when calling CoreV1Api->delete_namespaced_service: {e}"
            )
        #try:
        #    logger.info("delete postgresql instance service from k8s " +
        #                statefulset_name_get_external_service_name(
        #                    pod_name_get_statefulset_name(
        #                        conn.get_k8s().get_podname())))
        #    core_v1_api = client.CoreV1Api()
        #    delete_response = core_v1_api.delete_namespaced_service(
        #        statefulset_name_get_external_service_name(
        #            pod_name_get_statefulset_name(
        #                conn.get_k8s().get_podname())),
        #        conn.get_k8s().get_namespace())
        #except Exception as e:
        #    logger.error(
        #        "Exception when calling CoreV1Api->delete_namespaced_service: %s\n"
        #        % e)

    if delete_disk == True:
        delete_storage(meta, spec, patch, status, logger, conn)


def delete_autofailover(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    field: str,
    target_machines: List,
    target_k8s: List,  #[begin:int, end: int]
    delete_disk: bool,
) -> None:
    logger.info("delete autofailover instance")
    conns = pgsql_util.connections_target(meta, spec, patch, status, logger,
                                          field, target_machines, target_k8s)
    for conn in conns.get_conns():
        delete_postgresql(meta, spec, patch, status, logger, delete_disk, conn)
    conns.free_conns()


def delete_postgresql_readwrite(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    field: str,
    target_machines: List,
    target_k8s: List,  #[begin:int, end: int]
    delete_disk: bool,
) -> None:
    logger.info("delete postgresql readwrite instance")
    conns = pgsql_util.connections_target(meta, spec, patch, status, logger,
                                          field, target_machines, target_k8s)
    for conn in conns.get_conns():
        delete_postgresql(meta, spec, patch, status, logger, delete_disk, conn)
    conns.free_conns()


def delete_postgresql_readonly(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    field: str,
    target_machines: List,
    target_k8s: List,  #[begin:int, end: int]
    delete_disk: bool,
) -> None:
    logger.info("delete postgresql readonly instance")
    conns = pgsql_util.connections_target(meta, spec, patch, status, logger,
                                          field, target_machines, target_k8s)
    for conn in conns.get_conns():
        delete_postgresql(meta, spec, patch, status, logger, delete_disk, conn)
    conns.free_conns()


def delete_cluster(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> None:
    pgsql_util.set_cluster_status(meta, CLUSTER_STATE,
                                  CLUSTER_STATUS_TERMINATE, logger)
    delete_postgresql_cluster(meta, spec, patch, status, logger)


def delete_pvc(logger: logging.Logger, name: str, namespace: str) -> None:
    core_v1_api = client.CoreV1Api()
    grace_period_seconds = 0

    logger.info(f"delete pvc {name}")
    try:
        core_v1_api.delete_namespaced_persistent_volume_claim(
            name, namespace, grace_period_seconds=grace_period_seconds)
    except Exception as e:
        logger.error(
            "Exception when calling CoreV1Api->delete_namespaced_persistent_volume_claim: %s\n"
            % e)


def delete_storage(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    conn: InstanceConnection,
) -> None:

    if conn.get_k8s() != None:
        delete_pvc(logger,
                   pgsql_util.get_pvc_name(conn.get_k8s().get_podname()),
                   conn.get_k8s().get_namespace())
    if conn.get_machine() != None:
        pgsql_create.machine_postgresql_down(conn, logger)
        logger.info("delete machine disk " + conn.get_machine().get_host())
        pgsql_util.postgresql_action(meta, spec, patch, status, logger, conn,
                                     False)
        if conn.get_machine().get_role() == AUTOFAILOVER:
            machine_data_path = operator_config.DATA_PATH_AUTOFAILOVER
        if conn.get_machine().get_role() == POSTGRESQL:
            machine_data_path = operator_config.DATA_PATH_POSTGRESQL
        pgsql_util.machine_exec_command(conn.get_machine().get_ssh(),
                                        "rm -rf " + machine_data_path)


def delete_storages(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> None:

    conns = pgsql_util.connections(spec, meta, patch,
                                   pgsql_util.get_field(AUTOFAILOVER), False,
                                   None, logger, None, status, False)
    for conn in conns.get_conns():
        delete_storage(meta, spec, patch, status, logger, conn)
    conns.free_conns()

    conns = pgsql_util.connections(
        spec, meta, patch, pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE),
        False, None, logger, None, status, False)
    for conn in conns.get_conns():
        delete_storage(meta, spec, patch, status, logger, conn)
    conns.free_conns()

    conns = pgsql_util.connections(
        spec, meta, patch, pgsql_util.get_field(POSTGRESQL, READONLYINSTANCE),
        False, None, logger, None, status, False)
    for conn in conns.get_conns():
        delete_storage(meta, spec, patch, status, logger, conn)
    conns.free_conns()


def delete_postgresql_cluster(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> None:
    if spec[SPEC_DELETE_S3]:
        pgsql_backup.delete_s3_backup_by_main_cr(
            meta,
            spec,
            patch,
            status,
            logger,
            backup_id=SPEC_BACKUPTOS3_POLICY_RETENTION_DELETE_ALL_VALUE)
    if spec[DELETE_PVC]:
        delete_storages(meta, spec, patch, status, logger)


def delete_services(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> None:
    autofailover_machines = spec.get(AUTOFAILOVER).get(MACHINES)
    # k8s mode
    if autofailover_machines == None:
        core_v1_api = client.CoreV1Api()
        try:
            api_response = core_v1_api.list_service_for_all_namespaces(
                label_selector=pgsql_util.dict_to_str(
                    pgsql_util.get_service_labels(meta)),
                watch=False)
            for service in api_response.items:
                try:
                    logger.info("delete service " + service.metadata.name)
                    delete_response = core_v1_api.delete_namespaced_service(
                        service.metadata.name, service.metadata.namespace)
                except Exception as e:
                    logger.error(
                        "Exception when calling CoreV1Api->delete_namespaced_service: %s\n"
                        % e)
        except Exception as e:
            logger.error(
                "Exception when calling CoreV1Api->list_service_for_all_namespaces: %s\n"
                % e)
    else:
        conns = pgsql_util.connections(
            spec, meta, patch,
            pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE), False, None,
            logger, None, status, False)
        readonly_conns = pgsql_util.connections(
            spec, meta, patch,
            pgsql_util.get_field(POSTGRESQL, READONLYINSTANCE), False, None,
            logger, None, status, False)
        for conn in (conns.get_conns() + readonly_conns.get_conns()):
            pgsql_util.machine_exec_command(conn.get_machine().get_ssh(),
                                            STOP_KEEPALIVED)
            pgsql_util.machine_exec_command(conn.get_machine().get_ssh(),
                                            "rm -rf " + KEEPALIVED_CONF)
            pgsql_util.machine_exec_command(conn.get_machine().get_ssh(),
                                            LVS_UNSET_NET)
        conns.free_conns()
        readonly_conns.free_conns()


def drop_one_user(conn: InstanceConnection, name: str,
                  logger: logging.Logger) -> None:
    cmd = "drop user if exists " + name + " "
    cmd = ["pgtools", "-q", '"' + cmd + '"']

    logger.info(f"drop postgresql user with cmd {cmd}")
    output = pgsql_util.exec_command(conn, cmd, logger, interrupt=False)
    if output != "DROP ROLE":
        logger.error(f"can't drop user {cmd}, {output}")
