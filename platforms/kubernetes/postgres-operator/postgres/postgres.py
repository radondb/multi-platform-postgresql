import logging
import kopf
import traceback
from constants import (
    API_GROUP,
    API_VERSION_V1,
    RESOURCE_POSTGRESQL,
)
from config import operator_config
from handle import create_cluster, delete_cluster, timer_cluster, update_cluster, daemon_cluster, set_cluster_status
from apscheduler.schedulers.background import BackgroundScheduler

from kubernetes import client, config

from constants import (
    SPEC_BACKUPCLUSTER,
    SPEC_BACKUPTOS3,
    SPEC_BACKUPTOS3_CRON,
    SPEC_BACKUPTOS3_CRON_ENABLE,
    CLUSTER_STATUS_CRON_NEXT_RUN,
)


@kopf.on.startup()
def startup(settings: kopf.OperatorSettings, **_kwargs):
    operator_config.load()

    # Timeout passed along to the Kubernetes API as timeoutSeconds=x
    settings.watching.server_timeout = 300
    # Total number of seconds for a whole watch request per aiohttp:
    # https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp.ClientTimeout.total
    settings.watching.client_timeout = 300
    # Timeout for attempting to connect to the peer per aiohttp:
    # https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp.ClientTimeout.sock_connect
    settings.watching.connect_timeout = 30
    # Wait for that many seconds between watching events
    settings.watching.reconnect_backoff = 1
    # setting the number of synchronous workers used by the operator for synchronous handlers
    settings.execution.max_workers = 30

    settings.peering.clusterwide = True

    if not operator_config.TESTING:
        # TODO start prometheus and other
        pass


# timeout: if create function run timeout large than timeout and no error. this is allow.
#          if create function run timeout large than timeout and error happend, it not retry,
#          if create function run timeout less than timeout and error happend, it do retry,
@kopf.on.create(API_GROUP,
                API_VERSION_V1,
                RESOURCE_POSTGRESQL,
                timeout=operator_config.BOOTSTRAP_TIMEOUT,
                retries=operator_config.BOOTSTRAP_RETRIES,
                backoff=operator_config.BOOTSTRAP_RETRY_DELAY)
def cluster_create(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    **_kwargs,
):

    create_cluster(meta, spec, patch, status, logger)


@kopf.on.update(API_GROUP,
                API_VERSION_V1,
                RESOURCE_POSTGRESQL,
                timeout=operator_config.BOOTSTRAP_TIMEOUT,
                retries=operator_config.BOOTSTRAP_RETRIES,
                backoff=operator_config.BOOTSTRAP_RETRY_DELAY)
def cluster_update(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    diff: kopf.Diff,
    **_kwargs,
):
    update_cluster(meta, spec, patch, status, logger, diff)


@kopf.on.delete(API_GROUP,
                API_VERSION_V1,
                RESOURCE_POSTGRESQL,
                timeout=operator_config.BOOTSTRAP_TIMEOUT,
                retries=operator_config.BOOTSTRAP_RETRIES,
                backoff=operator_config.BOOTSTRAP_RETRY_DELAY)
def cluster_delete(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    **_kwargs,
):
    delete_cluster(meta, spec, patch, status, logger)


# interval=operator_config.TIMER_INTERVAL can't set success
# interval only support int digital
@kopf.timer(
    API_GROUP,
    API_VERSION_V1,
    RESOURCE_POSTGRESQL,
    interval=60,
    sharp=True,
    initial_delay=10,
)
def cluster_timer(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    **_kwargs,
):
    timer_cluster(meta, spec, patch, status, logger)


@kopf.daemon(
    API_GROUP,
    API_VERSION_V1,
    RESOURCE_POSTGRESQL,
    backoff=operator_config.BOOTSTRAP_RETRY_DELAY,
    initial_delay=30,
    when=lambda spec, **_: spec.get(SPEC_BACKUPCLUSTER, {}).get(
        SPEC_BACKUPTOS3, {}).get(SPEC_BACKUPTOS3_CRON, {}).get(
            SPEC_BACKUPTOS3_CRON_ENABLE, False) is True,
)
def cluster_daemon(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    stopped: kopf.DaemonStopped,
    **_kwargs,
):

    # init and start BackgroundScheduler
    scheduler = BackgroundScheduler()
    job_defaults = {'coalesce': False, 'max_instances': 1}
    scheduler.configure(job_defaults)
    scheduler.start()

    try:
        while not stopped:
            daemon_cluster(meta, spec, patch, status, logger, scheduler)
            stopped.wait(60)
    except (ValueError, Exception):
        traceback.print_exc()
        traceback.format_exc()
    finally:
        logger.warning(
            f"cluster_daemon with name: {meta['name']}, namespace: {meta['namespace']}, spec: {spec} are done. remove all jobs and shutdown scheduler."
        )
        scheduler.remove_all_jobs()
        scheduler.shutdown()
        set_cluster_status(meta, CLUSTER_STATUS_CRON_NEXT_RUN, "", logger)
        raise Exception("cluster_daemon completed.")
