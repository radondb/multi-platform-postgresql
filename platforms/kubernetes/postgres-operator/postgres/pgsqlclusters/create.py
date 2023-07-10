import logging
import kopf
import os
import copy
import traceback
import time

from kubernetes import client

from pgsqlbackups.restore import restore_postgresql, is_restore_mode
from pgsqlcommons.config import operator_config
from pgsqlcommons.typed import LabelType, InstanceConnection, InstanceConnections, TypedDict, InstanceConnectionMachine, InstanceConnectionK8S, Tuple, Any, List
from pgsqlcommons.constants import *
from pgsqlclusters.utiles import (
    set_cluster_status,
    check_param,
    waiting_cluster_final_status,
    update_pgpassfile,
    update_number_sync_standbys,
    set_password,
    get_service_labels,
    get_readonly_labels,
    get_field,
    machine_exec_command,
    waiting_target_postgresql_ready,
    get_postgresql_config_port,
    machine_sftp_put,
    connections,
    get_autofailover_labels,
    get_readwrite_labels,
    get_service_autofailover_labels,
    get_service_primary_labels,
    get_service_standby_labels,
    get_service_readonly_labels,
    get_service_standby_readonly_labels,
    exec_command,
    get_antiaffinity,
    get_realimage_from_env,
    get_service_name,
    statefulset_name_get_service_name,
    get_primary_conn,
    get_machine_exporter_env,
    get_k8s_exporter_env,
    get_pod_address,
    waiting_postgresql_ready,
    waiting_pg_basebackup_completed,
    get_statefulset_name,
    waiting_instance_ready,
    statefulset_name_get_external_service_name,
)


def create_cluster(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> None:
    try:
        set_cluster_status(meta, CLUSTER_STATE, CLUSTER_STATUS_CREATE, logger)

        logging.info("check create_cluster params")
        check_param(spec, logger, create=True)
        create_postgresql_cluster(meta, spec, patch, status, logger)

        logger.info("waiting for create_cluster success")
        waiting_cluster_final_status(meta,
                                     spec,
                                     patch,
                                     status,
                                     logger,
                                     timeout=MINUTES * 5)

        update_pgpassfile(meta, spec, patch, status, logger)

        # wait a few seconds to prevent the pod not running
        time.sleep(5)
        # cluster running
        update_number_sync_standbys(meta, spec, patch, status, logger)
        set_cluster_status(meta, CLUSTER_STATE, CLUSTER_STATUS_RUN, logger)
    except Exception as e:
        logger.error(f"error occurs, {e}")
        traceback.print_exc()
        traceback.format_exc()
        set_cluster_status(meta, CLUSTER_STATE, CLUSTER_STATUS_CREATE_FAILED,
                           logger)


def create_postgresql_cluster(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> None:

    logger.info("creating postgresql cluster")
    set_password(patch, status)

    # create services
    create_services(meta, spec, patch, status, logger)

    # create autofailover
    # set_create_cluster(patch, CLUSTER_CREATE_ADD_FAILOVER)
    create_autofailover(meta, spec, patch, status, logger,
                        get_autofailover_labels(meta))

    # create postgresql & readwrite node
    # set_create_cluster(patch, CLUSTER_CREATE_ADD_READWRITE)
    create_postgresql_readwrite(meta, spec, patch, status, logger,
                                get_readwrite_labels(meta), 0, True)
    #conns = connections(spec, meta, patch,
    #                    get_field(POSTGRESQL, READWRITEINSTANCE), False, None,
    #                    logger, None, status, False)
    #if conns.get_conns()[0].get_machine() != None:
    #    waiting_postgresql_ready(conns, logger)
    #    waiting_cluster_final_status(meta, spec, patch, status, logger)
    #conns.free_conns()

    # create postgresql & readonly node
    # set_create_cluster(patch, CLUSTER_CREATE_ADD_READONLY)
    create_postgresql_readonly(meta, spec, patch, status, logger,
                               get_readonly_labels(meta), 0)

    # finish
    # set_create_cluster(patch, CLUSTER_CREATE_FINISH)


def create_statefulset_service(
    name: str,
    external_name: str,
    namespace: str,
    labels: LabelType,
    #port: int,
    logger: logging.Logger,
    meta: kopf.Meta,
) -> None:
    core_v1_api = client.CoreV1Api()

    statefulset_service_body = {}
    statefulset_service_body["apiVersion"] = "v1"
    statefulset_service_body["kind"] = "Service"
    statefulset_service_body["metadata"] = {
        "name": name,
        "namespace": namespace
        #"labels": get_statefulset_service_labels(meta)
    }
    statefulset_service_body["spec"] = {
        "clusterIP": "None",
        "selector": labels
        # allow all port
        #"ports": [{
        #    "port": port,
        #    "targetPort": port
        #}]
    }

    logger.info(f"create statefulset service with {statefulset_service_body}")
    kopf.adopt(statefulset_service_body)
    core_v1_api.create_namespaced_service(namespace=namespace,
                                          body=statefulset_service_body)

    #service_body = {}
    #service_body["apiVersion"] = "v1"
    #service_body["kind"] = "Service"
    #service_body["metadata"] = {
    #    "name": external_name,
    #    "namespace": namespace
    #}
    #service_body["spec"] = {
    #    "type": "ClusterIP",
    #    "selector": labels,
    #    "ports": [{
    #        "port": port,
    #        "targetPort": port
    #    }]
    #}

    #logger.info(f"create statefulset external service with {service_body}")
    #kopf.adopt(service_body)
    #core_v1_api.create_namespaced_service(namespace=namespace,
    #                                      body=service_body)


def create_statefulset(
    meta: kopf.Meta,
    spec: kopf.Spec,
    name: str,
    namespace: str,
    labels: LabelType,
    podspec_need_copy: TypedDict,
    vct: TypedDict,
    antiaffinity_need_copy: TypedDict,
    env: TypedDict,
    logger: logging.Logger,
    exporter_env: List,
) -> None:

    apps_v1_api = client.AppsV1Api()
    statefulset_body = {}
    statefulset_body["apiVersion"] = "apps/v1"
    statefulset_body["kind"] = "StatefulSet"
    statefulset_body["metadata"] = {"name": name, "namespace": namespace}
    statefulset_body["spec"] = {}
    statefulset_body_spec_selector = {"matchLabels": labels}
    statefulset_body["spec"]["selector"] = statefulset_body_spec_selector
    statefulset_body["spec"]["replicas"] = STATEFULSET_REPLICAS
    statefulset_body["spec"][
        "serviceName"] = statefulset_name_get_service_name(name)
    podspec = copy.deepcopy(podspec_need_copy)
    podspec["restartPolicy"] = "Always"
    podspec.setdefault(SPEC_POD_PRIORITY_CLASS,
                       SPEC_POD_PRIORITY_CLASS_SCOPE_CLUSTER)
    antiaffinity = copy.deepcopy(antiaffinity_need_copy)
    is_required = antiaffinity[
        SPEC_ANTIAFFINITY_POLICY] == SPEC_ANTIAFFINITY_REQUIRED
    antiaffinity = get_antiaffinity(meta, labels, antiaffinity)
    if antiaffinity:
        spec_antiaffinity = SPEC_ANTIAFFINITY_POLICY_REQUIRED if is_required else SPEC_ANTIAFFINITY_POLICY_PREFERRED
        podspec.setdefault("affinity", {}).setdefault("podAntiAffinity", {})[spec_antiaffinity] = \
            antiaffinity
    for container in podspec[CONTAINERS]:
        if container[
                CONTAINER_NAME] == PODSPEC_CONTAINERS_POSTGRESQL_CONTAINER:
            container["args"] = ["auto_failover"]
            container["env"] = env
            container["readinessProbe"] = {
                "initialDelaySeconds": 20,
                "periodSeconds": 5,
                "exec": {
                    "command": WAITING_POSTGRESQL_READY_COMMAND
                }
            }
        if container[CONTAINER_NAME] == PODSPEC_CONTAINERS_EXPORTER_CONTAINER:
            container["env"] = exporter_env
        container[IMAGE] = get_realimage_from_env(container[IMAGE])
    statefulset_body["spec"]["template"] = {
        "metadata": {
            "labels": labels
        },
        "spec": podspec
    }
    statefulset_body["spec"]["volumeClaimTemplates"] = vct

    logger.info(f"create statefulset with {statefulset_body}")
    kopf.adopt(statefulset_body)
    try:
        apps_v1_api.create_namespaced_stateful_set(namespace=namespace,
                                                   body=statefulset_body)
    except Exception as e:
        print(
            "Exception when calling AppsV1Api->create_namespaced_stateful_set: %s\n"
            % e)


def create_postgresql(
    mode: str,
    spec: kopf.Spec,
    meta: kopf.Meta,
    field: str,
    labels: LabelType,
    logger: logging.Logger,
    conns: InstanceConnections,
    patch: kopf.Patch,
    create_begin: int,
    status: kopf.Status,
    wait_primary: bool,
    create_end: int,
) -> None:

    logger.info("create postgresql on " + mode)

    antiaffinity = spec.get(SPEC_ANTIAFFINITY)
    if len(field.split(FIELD_DELIMITER)) == 1:
        localspec = spec.get(field)
        hbas = localspec[HBAS]
        configs = localspec[CONFIGS]
        machine_data_path = operator_config.DATA_PATH_AUTOFAILOVER
    else:
        if len(field.split(FIELD_DELIMITER)) != 2:
            raise kopf.PermanentError(
                "error parse field, only support one '.'" + field)
        localspec = spec.get(field.split(FIELD_DELIMITER)[0]).get(
            field.split(FIELD_DELIMITER)[1])
        hbas = spec[field.split(FIELD_DELIMITER)[0]][HBAS]
        configs = spec[field.split(FIELD_DELIMITER)[0]][CONFIGS]
        machine_data_path = operator_config.DATA_PATH_POSTGRESQL

    for container in localspec[PODSPEC][CONTAINERS]:
        if container[
                CONTAINER_NAME] == PODSPEC_CONTAINERS_POSTGRESQL_CONTAINER:
            postgresql_image = container[IMAGE]
            postgresql_image_version = postgresql_image.split(':')[1].split(
                '-')[1]
        if container[CONTAINER_NAME] == PODSPEC_CONTAINERS_EXPORTER_CONTAINER:
            exporter_image = container[IMAGE]

    if mode == MACHINE_MODE:
        replicas = conns.get_number() if create_end is None else create_end
        pgdata = os.path.join(machine_data_path, PGDATA_DIR)
        remotepath = os.path.join(machine_data_path, DOCKER_COMPOSE_DIR)
        machine_env = ""
        machine_exporter_env = get_machine_exporter_env(
            meta, spec, patch, status, logger, field,
            localspec[PODSPEC][CONTAINERS][EXPORTER_CONTAINER_INDEX])
    else:
        replicas = localspec.get(
            REPLICAS) if create_end is None else create_end
        if replicas == None:
            replicas = 1
        k8s_env = []
        k8s_exporter_env = get_k8s_exporter_env(
            meta, spec, patch, status, logger, field,
            localspec[PODSPEC][CONTAINERS][EXPORTER_CONTAINER_INDEX])

    for i, hba in enumerate(hbas):
        env_name = PG_HBA_PREFIX + str(i)
        env_value = hba
        if mode == MACHINE_MODE:
            machine_env += env_name + "=" + env_value + "\n"
        else:
            k8s_env.append({
                CONTAINER_ENV_NAME: env_name,
                CONTAINER_ENV_VALUE: env_value
            })

    for config in configs:
        name = config.split("=")[0].strip()
        value = config[config.find("=") + 1:].strip()

        if name in PG_CONFIG_IGNORE:
            continue

        if field == get_field(AUTOFAILOVER) and name == 'port':
            value = str(AUTO_FAILOVER_PORT)

        config = name + "=" + value
        if mode == MACHINE_MODE:
            machine_env += PG_CONFIG_PREFIX + config + "\n"
        else:
            k8s_env.append({
                CONTAINER_ENV_NAME: PG_CONFIG_PREFIX + name,
                CONTAINER_ENV_VALUE: value
            })
    if postgresql_image_version == POSTGRESQL_IMAGE_VERSION_v1_1_0:
        pgaudit = ''
    else:
        pgaudit = ',pgaudit'

    if mode == MACHINE_MODE:
        machine_env += PG_CONFIG_PREFIX + f"shared_preload_libraries='citus,pgautofailover,pg_stat_statements{pgaudit}'" + "\n"
        machine_env += PG_CONFIG_PREFIX + 'log_truncate_on_rotation=true' + "\n"
        machine_env += PG_CONFIG_PREFIX + 'logging_collector=on' + "\n"
        machine_env += PG_CONFIG_PREFIX + "log_directory='" + PGLOG_DIR + "'" + "\n"
        machine_env += PG_CONFIG_PREFIX + "log_filename='postgresql_%d'" + "\n"
        machine_env += PG_CONFIG_PREFIX + "log_line_prefix='[%m][%r][%a][%u][%d][%x][%p]'" + "\n"
        machine_env += PG_CONFIG_PREFIX + "log_destination='csvlog'" + "\n"
        machine_env += PG_CONFIG_PREFIX + 'log_autovacuum_min_duration=-1' + "\n"
        machine_env += PG_CONFIG_PREFIX + "log_timezone='Asia/Shanghai'" + "\n"
        machine_env += PG_CONFIG_PREFIX + "datestyle='iso, ymd'" + "\n"
        machine_env += PG_CONFIG_PREFIX + "timezone='Asia/Shanghai'" + "\n"
        machine_env += PG_CONFIG_PREFIX + "tcp_keepalives_idle=60" + "\n"
        machine_env += PG_CONFIG_PREFIX + "tcp_keepalives_interval=30" + "\n"
        machine_env += PG_CONFIG_PREFIX + "tcp_keepalives_count=4" + "\n"
        machine_env += PG_CONFIG_PREFIX + "archive_mode=on" + "\n"
        machine_env += PG_CONFIG_PREFIX + "archive_command='/bin/true'" + "\n"
    else:
        k8s_env.append({
            CONTAINER_ENV_NAME:
            PG_CONFIG_PREFIX + "shared_preload_libraries",
            CONTAINER_ENV_VALUE:
            f"'citus,pgautofailover,pg_stat_statements{pgaudit}'"
        })
        k8s_env.append({
            CONTAINER_ENV_NAME: PG_CONFIG_PREFIX + "log_truncate_on_rotation",
            CONTAINER_ENV_VALUE: "true"
        })
        k8s_env.append({
            CONTAINER_ENV_NAME: PG_CONFIG_PREFIX + "logging_collector",
            CONTAINER_ENV_VALUE: "on"
        })
        k8s_env.append({
            CONTAINER_ENV_NAME: PG_CONFIG_PREFIX + "log_directory",
            CONTAINER_ENV_VALUE: "'" + PGLOG_DIR + "'"
        })
        k8s_env.append({
            CONTAINER_ENV_NAME: PG_CONFIG_PREFIX + "log_filename",
            CONTAINER_ENV_VALUE: "'postgresql_%d'"
        })
        k8s_env.append({
            CONTAINER_ENV_NAME: PG_CONFIG_PREFIX + "log_line_prefix",
            CONTAINER_ENV_VALUE: "'[%m][%r][%a][%u][%d][%x][%p]'"
        })
        k8s_env.append({
            CONTAINER_ENV_NAME: PG_CONFIG_PREFIX + "log_destination",
            CONTAINER_ENV_VALUE: "'csvlog'"
        })
        k8s_env.append({
            CONTAINER_ENV_NAME:
            PG_CONFIG_PREFIX + "log_autovacuum_min_duration",
            CONTAINER_ENV_VALUE: "-1"
        })
        k8s_env.append({
            CONTAINER_ENV_NAME: PG_CONFIG_PREFIX + "log_timezone",
            CONTAINER_ENV_VALUE: "'Asia/Shanghai'"
        })
        k8s_env.append({
            CONTAINER_ENV_NAME: PG_CONFIG_PREFIX + "datestyle",
            CONTAINER_ENV_VALUE: "'iso, ymd'"
        })
        k8s_env.append({
            CONTAINER_ENV_NAME: PG_CONFIG_PREFIX + "timezone",
            CONTAINER_ENV_VALUE: "'Asia/Shanghai'"
        })
        k8s_env.append({
            CONTAINER_ENV_NAME: PG_CONFIG_PREFIX + "tcp_keepalives_idle",
            CONTAINER_ENV_VALUE: "60"
        })
        k8s_env.append({
            CONTAINER_ENV_NAME: PG_CONFIG_PREFIX + "tcp_keepalives_interval",
            CONTAINER_ENV_VALUE: "30"
        })
        k8s_env.append({
            CONTAINER_ENV_NAME: PG_CONFIG_PREFIX + "tcp_keepalives_count",
            CONTAINER_ENV_VALUE: "4"
        })
        k8s_env.append({
            CONTAINER_ENV_NAME: PG_CONFIG_PREFIX + "archive_mode",
            CONTAINER_ENV_VALUE: "on"
        })
        k8s_env.append({
            CONTAINER_ENV_NAME: PG_CONFIG_PREFIX + "archive_command",
            CONTAINER_ENV_VALUE: "'/bin/true'"
        })
    for replica in range(create_begin, replicas):
        name = get_statefulset_name(meta["name"], field, replica)
        namespace = meta["namespace"]
        autoctl_node_password = patch.status.get(AUTOCTL_NODE)
        if autoctl_node_password == None:
            autoctl_node_password = status.get(AUTOCTL_NODE)
        autoctl_replicator_password = patch.status.get(
            PGAUTOFAILOVER_REPLICATOR)
        if autoctl_replicator_password == None:
            autoctl_replicator_password = status.get(PGAUTOFAILOVER_REPLICATOR)

        if field == get_field(AUTOFAILOVER):
            if mode == MACHINE_MODE:
                machine_env += "PG_MODE=monitor\n"
                machine_env += "AUTOCTL_NODE_PASSWORD=" + autoctl_node_password + "\n"
                machine_env += "EXTERNAL_HOSTNAME=" + conns.get_conns(
                )[replica].get_machine().get_host() + "\n"
            else:
                k8s_env.append({
                    CONTAINER_ENV_NAME: "PG_MODE",
                    CONTAINER_ENV_VALUE: "monitor"
                })
                k8s_env.append({
                    CONTAINER_ENV_NAME: "AUTOCTL_NODE_PASSWORD",
                    CONTAINER_ENV_VALUE: autoctl_node_password
                })
                k8s_env.append({
                    CONTAINER_ENV_NAME:
                    "EXTERNAL_HOSTNAME",
                    CONTAINER_ENV_VALUE:
                    get_pod_address(meta["name"], field, replica, namespace)
                })

        if field == get_field(POSTGRESQL, READWRITEINSTANCE):
            if mode == MACHINE_MODE:
                auto_failover_conns = connections(spec, meta, patch,
                                                  get_field(AUTOFAILOVER),
                                                  False, None, logger, None,
                                                  status, False)
                auto_failover_host = auto_failover_conns.get_conns(
                )[0].get_machine().get_host()
                auto_failover_conns.free_conns()
                machine_env += "PG_MODE=readwrite\n"
                machine_env += "AUTOCTL_NODE_PASSWORD=" + autoctl_node_password + "\n"
                machine_env += "EXTERNAL_HOSTNAME=" + conns.get_conns(
                )[replica].get_machine().get_host() + "\n"
                machine_env += "AUTOCTL_REPLICATOR_PASSWORD=" + autoctl_replicator_password + "\n"
                machine_env += "MONITOR_HOSTNAME=" + auto_failover_host + "\n"
            else:
                k8s_env.append({
                    CONTAINER_ENV_NAME: "PG_MODE",
                    CONTAINER_ENV_VALUE: "readwrite"
                })
                k8s_env.append({
                    CONTAINER_ENV_NAME: "AUTOCTL_NODE_PASSWORD",
                    CONTAINER_ENV_VALUE: autoctl_node_password
                })
                k8s_env.append({
                    CONTAINER_ENV_NAME:
                    "EXTERNAL_HOSTNAME",
                    CONTAINER_ENV_VALUE:
                    get_pod_address(meta["name"], field, replica, namespace)
                })
                k8s_env.append({
                    CONTAINER_ENV_NAME: "AUTOCTL_REPLICATOR_PASSWORD",
                    CONTAINER_ENV_VALUE: autoctl_replicator_password
                })
                k8s_env.append({
                    CONTAINER_ENV_NAME:
                    "MONITOR_HOSTNAME",
                    CONTAINER_ENV_VALUE:
                    get_pod_address(meta["name"], AUTOFAILOVER, 0, namespace)
                })
        if field == get_field(POSTGRESQL, READONLYINSTANCE):
            if mode == MACHINE_MODE:
                auto_failover_conns = connections(spec, meta, patch,
                                                  get_field(AUTOFAILOVER),
                                                  False, None, logger, None,
                                                  status, False)
                auto_failover_host = auto_failover_conns.get_conns(
                )[0].get_machine().get_host()
                auto_failover_conns.free_conns()
                machine_env += "PG_MODE=readonly\n"
                machine_env += "AUTOCTL_NODE_PASSWORD=" + autoctl_node_password + "\n"
                machine_env += "EXTERNAL_HOSTNAME=" + conns.get_conns(
                )[replica].get_machine().get_host() + "\n"
                machine_env += "AUTOCTL_REPLICATOR_PASSWORD=" + autoctl_replicator_password + "\n"
                machine_env += "PG_STREAMING=" + localspec[STREAMING] + "\n"
                machine_env += "MONITOR_HOSTNAME=" + auto_failover_host + "\n"
            else:
                k8s_env.append({
                    CONTAINER_ENV_NAME: "PG_MODE",
                    CONTAINER_ENV_VALUE: "readonly"
                })
                k8s_env.append({
                    CONTAINER_ENV_NAME: "AUTOCTL_NODE_PASSWORD",
                    CONTAINER_ENV_VALUE: autoctl_node_password
                })
                k8s_env.append({
                    CONTAINER_ENV_NAME:
                    "EXTERNAL_HOSTNAME",
                    CONTAINER_ENV_VALUE:
                    get_pod_address(meta["name"], field, replica, namespace)
                })
                k8s_env.append({
                    CONTAINER_ENV_NAME: "AUTOCTL_REPLICATOR_PASSWORD",
                    CONTAINER_ENV_VALUE: autoctl_replicator_password
                })
                k8s_env.append({
                    CONTAINER_ENV_NAME: "PG_STREAMING",
                    CONTAINER_ENV_VALUE: localspec[STREAMING]
                })
                k8s_env.append({
                    CONTAINER_ENV_NAME:
                    "MONITOR_HOSTNAME",
                    CONTAINER_ENV_VALUE:
                    get_pod_address(meta["name"], AUTOFAILOVER, 0, namespace)
                })

        if mode == MACHINE_MODE:
            logger.info("put docker-compose file to remote")
            machine_sftp_put(
                conns.get_conns()[replica].get_machine().get_sftp(),
                DOCKER_COMPOSE_FILE_DATA %
                (conns.get_conns()[replica].get_machine().get_role(),
                 conns.get_conns()[replica].get_machine().get_role(),
                 conns.get_conns()[replica].get_machine().get_role() +
                 PODSPEC_CONTAINERS_EXPORTER_CONTAINER,
                 conns.get_conns()[replica].get_machine().get_role() +
                 PODSPEC_CONTAINERS_EXPORTER_CONTAINER),
                os.path.join(remotepath, DOCKER_COMPOSE_FILE))
            machine_sftp_put(
                conns.get_conns()[replica].get_machine().get_sftp(),
                DOCKER_COMPOSE_ENV_DATA.format(
                    postgresql_image,
                    conns.get_conns()[replica].get_machine().get_host(),
                    pgdata, exporter_image),
                os.path.join(remotepath, DOCKER_COMPOSE_ENV))
            machine_sftp_put(
                conns.get_conns()[replica].get_machine().get_sftp(),
                machine_env, os.path.join(remotepath, DOCKER_COMPOSE_ENVFILE))
            machine_sftp_put(
                conns.get_conns()[replica].get_machine().get_sftp(),
                machine_exporter_env,
                os.path.join(remotepath, DOCKER_COMPOSE_EXPORTER_ENVFILE))

            logger.info("start with docker-compose")
            machine_exec_command(
                conns.get_conns()[replica].get_machine().get_ssh(),
                "cd " + os.path.join(machine_data_path, DOCKER_COMPOSE_DIR) +
                "; docker-compose up -d")
        else:
            create_statefulset_service(
                statefulset_name_get_service_name(name),
                statefulset_name_get_external_service_name(name), namespace,
                labels, logger, meta)
            create_statefulset(meta, spec, name, namespace, labels,
                               localspec[PODSPEC],
                               localspec[VOLUMECLAIMTEMPLATES], antiaffinity,
                               k8s_env, logger, k8s_exporter_env)
        # waiting pull image success and instance ready
        waiting_instance_ready(conns, logger, replica, replica + 1)

        # wait primary node create finish
        if wait_primary == True and field == get_field(
                POSTGRESQL, READWRITEINSTANCE) and replica == 0:
            # don't free the tmpconns
            tmpconn = conns.get_conns()[0]
            if is_restore_mode(meta, spec, patch, status, logger) == False:
                tmpconns: InstanceConnections = InstanceConnections()
                tmpconns.add(tmpconn)
                waiting_postgresql_ready(tmpconns, logger, timeout=MINUTES * 5)

                create_log_table(
                    logger, tmpconn,
                    int(
                        postgresql_image.split(':')[1].split('-')[0].split('.')
                        [0]))

                create_users(meta, spec, patch, status, logger, tmpconns)
            else:
                restore_postgresql(meta, spec, patch, status, logger, tmpconn)

    waiting_pg_basebackup_completed(conns, logger, create_begin, replicas)


def create_services(
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
        for service in spec[SERVICES]:
            autofailover = False
            if service[SELECTOR] == SERVICE_AUTOFAILOVER:
                labels = get_service_autofailover_labels(meta)
                autofailover = True
            elif service[SELECTOR] == SERVICE_PRIMARY:
                labels = get_service_primary_labels(meta)
            elif service[SELECTOR] == SERVICE_STANDBY:
                labels = get_service_standby_labels(meta)
            elif service[SELECTOR] == SERVICE_READONLY:
                labels = get_service_readonly_labels(meta)
            elif service[SELECTOR] == SERVICE_STANDBY_READONLY:
                labels = get_service_standby_readonly_labels(meta)
            else:
                raise kopf.PermanentError("unknow the services selector " +
                                          service[SELECTOR])

            service_body = {}
            service_body["apiVersion"] = "v1"
            service_body["kind"] = "Service"
            service_body["metadata"] = {
                "name": get_service_name(meta, service),
                "namespace": meta["namespace"],
                "labels": get_service_labels(meta)
            }

            service_body["spec"] = service["spec"]
            if autofailover == False:
                for port in service_body["spec"]["ports"]:
                    if port["name"] == PRIME_SERVICE_PORT_NAME:
                        postgresql_config_port = int(port['port'])
                postgresql_config_port = get_postgresql_config_port(
                    meta, spec, patch, status, logger)
                for port in service_body["spec"]["ports"]:
                    if port["name"] == PRIME_SERVICE_PORT_NAME:
                        port["targetPort"] = postgresql_config_port
                    if port["name"] == EXPORTER_SERVICE_PORT_NAME:
                        if port.get("targetPort") == None:
                            port["targetPort"] = EXPORTER_PORT
            else:
                for port in service_body["spec"]["ports"]:
                    if port["name"] == PRIME_SERVICE_PORT_NAME:
                        port["targetPort"] = AUTO_FAILOVER_PORT
                    if port["name"] == EXPORTER_SERVICE_PORT_NAME:
                        if port.get("targetPort") == None:
                            port["targetPort"] = EXPORTER_PORT

            service_body["spec"]["selector"] = labels

            logger.info(f"create service with {service_body}")
            kopf.adopt(service_body)
            core_v1_api.create_namespaced_service(namespace=meta["namespace"],
                                                  body=service_body)

    else:
        real_main_servers = []
        real_read_servers = []
        main_vip = ""
        read_vip = ""

        for service in spec[SERVICES]:
            if service[SELECTOR] == SERVICE_PRIMARY:
                machines = spec.get(POSTGRESQL).get(READWRITEINSTANCE).get(
                    MACHINES)
                main_vip = service[VIP]
            elif service[SELECTOR] == SERVICE_READONLY:
                machines = spec.get(POSTGRESQL).get(READONLYINSTANCE).get(
                    MACHINES)
                read_vip = service[VIP]
                READ_SERVER = LVS_REAL_READ_SERVER
            elif service[SELECTOR] == SERVICE_STANDBY_READONLY:
                machines = copy.deepcopy(
                    spec.get(POSTGRESQL).get(READWRITEINSTANCE).get(MACHINES))
                machines += spec.get(POSTGRESQL).get(READONLYINSTANCE).get(
                    MACHINES)
                read_vip = service[VIP]
                READ_SERVER = LVS_REAL_READ_SERVER
            else:
                logger.error(f"unsupport service {service}")
                continue

            if machines == None or len(machines) == 0:
                machines = [
                    spec.get(POSTGRESQL).get(READWRITEINSTANCE).get(MACHINES)
                    [0]
                ]
                READ_SERVER = LVS_REAL_EMPTY_SERVER

            for machine in machines:
                if service[SELECTOR] == SERVICE_PRIMARY:
                    real_main_servers.append(
                        LVS_REAL_MAIN_SERVER.format(
                            ip=machine.split(':')[2],
                            port=get_postgresql_config_port(
                                meta, spec, patch, status, logger)))
                else:
                    real_read_servers.append(
                        READ_SERVER.format(ip=machine.split(':')[2],
                                           port=get_postgresql_config_port(
                                               meta, spec, patch, status,
                                               logger)))

        lvs_conf = LVS_BODY.format(
            net="eth0",
            main_vip=main_vip,
            read_vip=read_vip,
            routeid=hash(main_vip) % 255 + 1,
            port=get_postgresql_config_port(meta, spec, patch, status, logger),
            real_main_servers="\n".join(real_main_servers),
            real_read_servers="\n".join(real_read_servers))

        conns = connections(spec, meta, patch,
                            get_field(POSTGRESQL, READWRITEINSTANCE), False,
                            None, logger, None, status, False)
        readonly_conns = connections(spec, meta, patch,
                                     get_field(POSTGRESQL, READONLYINSTANCE),
                                     False, None, logger, None, status, False)
        for conn in (conns.get_conns() + readonly_conns.get_conns()):
            machine_sftp_put(conn.get_machine().get_sftp(), lvs_conf,
                             KEEPALIVED_CONF)
            machine_exec_command(
                conn.get_machine().get_ssh(),
                LVS_SET_NET.format(main_vip=main_vip, read_vip=read_vip))
            machine_exec_command(conn.get_machine().get_ssh(),
                                 START_KEEPALIVED)
        conns.free_conns()
        readonly_conns.free_conns()


def create_autofailover(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    labels: LabelType,
) -> None:
    logger.info("create autofailover")
    conns = connections(spec, meta, patch, get_field(AUTOFAILOVER), True,
                        labels, logger, 0, status, False)
    after_create_autofailover(meta, spec, patch, status, logger, conns)
    conns.free_conns()


def after_create_autofailover(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    conns: InstanceConnections,
) -> None:
    logger.info("after create autofailover")
    waiting_target_postgresql_ready(meta,
                                    spec,
                                    patch,
                                    get_field(AUTOFAILOVER),
                                    status,
                                    logger,
                                    timeout=MINUTES * 5)

    create_extension(conns.get_conns()[0], logger)


def create_extension(conn: InstanceConnection,
                     logger: logging.Logger,
                     dbname: str = "pg_auto_failover") -> None:
    logger.info("create postgresql extension")
    cmd = [
        "pgtools", "-Q", dbname, "-q",
        '"create extension if not exists pg_stat_statements"'
    ]
    output = exec_command(conn, cmd, logger, interrupt=False)
    if output.find("CREATE EXTENSION") == -1:
        logger.error(
            f"can't create pg_stat_statements extension with {cmd}, {output}")


# LABEL: MULTI_PG_VERSIONS
def create_log_table(logger: logging.Logger, conn: InstanceConnection,
                     postgresql_major_version: int) -> None:
    logger.info("create postgresql log table")
    cmd = ["truncate", "--size", "0", "%s/%s/*" % (PG_DATABASE_DIR, PGLOG_DIR)]
    output = exec_command(conn, cmd, logger, interrupt=False)

    cmd = ["pgtools", "-q", '"create extension file_fdw"']
    output = exec_command(conn, cmd, logger, interrupt=False)
    if output.find("CREATE EXTENSION") == -1:
        logger.error(f"can't create file_fdw {cmd}, {output}")

    cmd = [
        "pgtools", "-q",
        '"create server pg_file_server foreign data wrapper file_fdw"'
    ]
    output = exec_command(conn, cmd, logger, interrupt=False)
    if output.find("CREATE SERVER") == -1:
        #logging.getLogger().error(
        logger.error(f"can't create pg_file_server {cmd}, {output}")

    for day in range(1, 32):
        table_name = 'log_postgresql_' + "%02d" % day
        log_filepath = PGLOG_DIR + "/postgresql_" + "%02d" % day + '.csv'
        if postgresql_major_version == 12:
            query = """ CREATE foreign TABLE %s
                (
                     log_time timestamp(3),
                     user_name text,
                     database_name text,
                     process_id integer,
                     connection_from text,
                     session_id text,
                     session_line_num bigint,
                     command_tag text,
                     session_start_time timestamp,
                     virtual_transaction_id text,
                     transaction_id bigint,
                     error_severity text,
                     sql_state_code text,
                     message text,
                     detail text,
                     hint text,
                     internal_query text,
                     internal_query_pos integer,
                     context text,
                     query text,
                     query_pos integer,
                     location text,
                     application_name text
                ) server pg_file_server options(program 'grep -v pg_auto_failover %s',format 'csv',header 'true') """ % (
                table_name, log_filepath)
        elif postgresql_major_version == 13:
            query = """ CREATE foreign TABLE %s
                (
                     log_time timestamp(3),
                     user_name text,
                     database_name text,
                     process_id integer,
                     connection_from text,
                     session_id text,
                     session_line_num bigint,
                     command_tag text,
                     session_start_time timestamp,
                     virtual_transaction_id text,
                     transaction_id bigint,
                     error_severity text,
                     sql_state_code text,
                     message text,
                     detail text,
                     hint text,
                     internal_query text,
                     internal_query_pos integer,
                     context text,
                     query text,
                     query_pos integer,
                     location text,
                     application_name text,
                     backend_type text
                ) server pg_file_server options(program 'grep -v pg_auto_failover %s',format 'csv',header 'true') """ % (
                table_name, log_filepath)
        elif postgresql_major_version == 14 or postgresql_major_version == 15:
            query = """ CREATE foreign TABLE %s
                (
                     log_time timestamp(3),
                     user_name text,
                     database_name text,
                     process_id integer,
                     connection_from text,
                     session_id text,
                     session_line_num bigint,
                     command_tag text,
                     session_start_time timestamp,
                     virtual_transaction_id text,
                     transaction_id bigint,
                     error_severity text,
                     sql_state_code text,
                     message text,
                     detail text,
                     hint text,
                     internal_query text,
                     internal_query_pos integer,
                     context text,
                     query text,
                     query_pos integer,
                     location text,
                     application_name text,
                     backend_type text,
                     leader_pid integer,
                     query_id bigint
                ) server pg_file_server options(program 'grep -v pg_auto_failover %s',format 'csv',header 'true') """ % (
                table_name, log_filepath)
        else:
            logger.warning(
                f"no compatible postgresql version {postgresql_major_version}, create log with postgresql 14 query."
            )
            query = """ CREATE foreign TABLE %s
                (
                     log_time timestamp(3),
                     user_name text,
                     database_name text,
                     process_id integer,
                     connection_from text,
                     session_id text,
                     session_line_num bigint,
                     command_tag text,
                     session_start_time timestamp,
                     virtual_transaction_id text,
                     transaction_id bigint,
                     error_severity text,
                     sql_state_code text,
                     message text,
                     detail text,
                     hint text,
                     internal_query text,
                     internal_query_pos integer,
                     context text,
                     query text,
                     query_pos integer,
                     location text,
                     application_name text,
                     backend_type text,
                     leader_pid integer,
                     query_id bigint
                ) server pg_file_server options(program 'grep -v pg_auto_failover %s',format 'csv',header 'true') """ % (
                table_name, log_filepath)

        logger.info(f"create postgresql log table {table_name} query {query}")
        cmd = ["pgtools", "-q", '"' + query + '"']
        output = exec_command(conn, cmd, logger, interrupt=False)
        if output.find("CREATE FOREIGN TABLE") == -1:
            logger.error(f"can't create table {cmd}, {output}")


def machine_postgresql_down(conn: InstanceConnection,
                            logger: logging.Logger) -> None:
    if conn.get_machine().get_role() == AUTOFAILOVER:
        machine_data_path = operator_config.DATA_PATH_AUTOFAILOVER
    if conn.get_machine().get_role() == POSTGRESQL:
        machine_data_path = operator_config.DATA_PATH_POSTGRESQL

    cmd = "cd " + os.path.join(machine_data_path,
                               DOCKER_COMPOSE_DIR) + "; docker-compose down"
    logger.info("delete postgresql instance from machine " +
                conn.get_machine().get_host())
    machine_exec_command(conn.get_machine().get_ssh(), cmd)


def create_postgresql_readwrite(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    labels: LabelType,
    create_begin: int,
    wait_primary: bool,
    create_end: int = None,
) -> None:
    logger.info("create postgresql readwrite instance")
    conns = connections(spec, meta, patch,
                        get_field(POSTGRESQL, READWRITEINSTANCE), True, labels,
                        logger, create_begin, status, wait_primary, create_end)
    conns.free_conns()


def create_postgresql_readonly(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    labels: LabelType,
    create_begin: int,
    create_end: int = None,
) -> None:
    logger.info("create postgresql readonly instance")
    conns = connections(spec, meta, patch,
                        get_field(POSTGRESQL, READONLYINSTANCE), True, labels,
                        logger, create_begin, status, False, create_end)
    conns.free_conns()


def create_one_user(conn: InstanceConnection, name: str, password: str,
                    superuser: bool, logger: logging.Logger) -> None:
    cmd = "create user " + name + " "
    if superuser:
        cmd = cmd + " SUPERUSER CREATEROLE REPLICATION CREATEDB "
    cmd = cmd + " password '" + password + "'"
    cmd = ["pgtools", "-q", '"' + cmd + '"']

    logger.info(f"create postgresql user with cmd {cmd}")
    output = exec_command(conn, cmd, logger, interrupt=False)
    if output != "CREATE ROLE":
        logger.error(f"can't create user {cmd}, {output}")


def create_users_admin(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    conns: InstanceConnections,
) -> None:
    if spec[POSTGRESQL].get(SPEC_POSTGRESQL_USERS) == None:
        return

    conn = get_primary_conn(conns, 0, logger)

    if spec[POSTGRESQL][SPEC_POSTGRESQL_USERS].get(
            SPEC_POSTGRESQL_USERS_ADMIN) != None:
        admin_users = spec[POSTGRESQL][SPEC_POSTGRESQL_USERS][
            SPEC_POSTGRESQL_USERS_ADMIN]
        for user in admin_users:
            create_one_user(conn, user[SPEC_POSTGRESQL_USERS_USER_NAME],
                            user[SPEC_POSTGRESQL_USERS_USER_PASSWORD], True,
                            logger)


def create_users_maintenance(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    conns: InstanceConnections,
) -> None:
    if spec[POSTGRESQL].get(SPEC_POSTGRESQL_USERS) == None:
        return

    conn = get_primary_conn(conns, 0, logger)
    auto_failover_conns = connections(spec, meta, patch,
                                      get_field(AUTOFAILOVER), False, None,
                                      logger, None, status, False)
    auto_failover_conn = auto_failover_conns.get_conns()[0]

    if spec[POSTGRESQL][SPEC_POSTGRESQL_USERS].get(
            SPEC_POSTGRESQL_USERS_MAINTENANCE) != None:
        maintenance_users = spec[POSTGRESQL][SPEC_POSTGRESQL_USERS][
            SPEC_POSTGRESQL_USERS_MAINTENANCE]
        for user in maintenance_users:
            create_one_user(conn, user[SPEC_POSTGRESQL_USERS_USER_NAME],
                            user[SPEC_POSTGRESQL_USERS_USER_PASSWORD], True,
                            logger)
            create_one_user(auto_failover_conn,
                            user[SPEC_POSTGRESQL_USERS_USER_NAME],
                            user[SPEC_POSTGRESQL_USERS_USER_PASSWORD], True,
                            logger)
    auto_failover_conns.free_conns()


def create_users_normal(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    conns: InstanceConnections,
) -> None:
    if spec[POSTGRESQL].get(SPEC_POSTGRESQL_USERS) == None:
        return

    conn = get_primary_conn(conns, 0, logger)

    if spec[POSTGRESQL][SPEC_POSTGRESQL_USERS].get(
            SPEC_POSTGRESQL_USERS_NORMAL) != None:
        normal_users = spec[POSTGRESQL][SPEC_POSTGRESQL_USERS][
            SPEC_POSTGRESQL_USERS_NORMAL]
        for user in normal_users:
            create_one_user(conn, user[SPEC_POSTGRESQL_USERS_USER_NAME],
                            user[SPEC_POSTGRESQL_USERS_USER_PASSWORD], False,
                            logger)


def create_users(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    conns: InstanceConnections,
) -> None:
    if spec[POSTGRESQL].get(SPEC_POSTGRESQL_USERS) == None:
        return

    create_users_admin(meta, spec, patch, status, logger, conns)
    create_users_maintenance(meta, spec, patch, status, logger, conns)
    create_users_normal(meta, spec, patch, status, logger, conns)
