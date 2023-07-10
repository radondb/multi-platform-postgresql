import logging
import kopf
import traceback
from pgsqlcommons.constants import (
    API_GROUP,
    API_VERSION_V1,
    RESOURCE_POSTGRESQL,
    SPEC_BACKUPCLUSTER,
    RESOURCE_POSTGRESQL_BACKUP,
    SPEC_BACKUPTOS3,
    SPEC_BACKUPTOS3_CRON,
    SPEC_BACKUPTOS3_CRON_ENABLE,
    CLUSTER_STATUS_CRON_NEXT_RUN,
    DAYS,
    HOURS,
)

from pgsqlcommons.config import operator_config
from pgsqlclusters.create import create_cluster
from pgsqlclusters.update import update_cluster
from pgsqlclusters.delete import delete_cluster
from pgsqlclusters.timer import timer_cluster
from pgsqlclusters.daemon import daemon_cluster
from pgsqlclusters.utiles import set_cluster_status
from pgsqlbackups.main import create_backup, delete_backup, check_backup, daemon_backup
from pgsqlbackups.constants import *
from apscheduler.schedulers.background import BackgroundScheduler


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
    settings.execution.max_workers = 1000

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


############################################################
#                   backup entrypoint
############################################################
@kopf.on.create(API_GROUP,
                API_VERSION_V1,
                RESOURCE_POSTGRESQL_BACKUP,
                timeout=operator_config.BOOTSTRAP_TIMEOUT,
                retries=operator_config.BOOTSTRAP_RETRIES,
                backoff=operator_config.BOOTSTRAP_RETRY_DELAY)
def backup_create(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    **_kwargs,
):

    create_backup(meta, spec, patch, status, logger)


@kopf.on.delete(API_GROUP,
                API_VERSION_V1,
                RESOURCE_POSTGRESQL_BACKUP,
                timeout=operator_config.BOOTSTRAP_TIMEOUT,
                retries=operator_config.BOOTSTRAP_RETRIES,
                backoff=operator_config.BOOTSTRAP_RETRY_DELAY)
def backup_delete(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    **_kwargs,
):

    delete_backup(meta, spec, patch, status, logger)


@kopf.timer(
    API_GROUP,
    API_VERSION_V1,
    RESOURCE_POSTGRESQL_BACKUP,
    interval=DAYS,
    initial_delay=HOURS,
    retries=operator_config.BOOTSTRAP_RETRIES,
    backoff=operator_config.BOOTSTRAP_RETRY_DELAY,
)
def backup_check(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    **_kwargs,
):

    check_backup(meta, spec, patch, status, logger)


@kopf.daemon(
    API_GROUP,
    API_VERSION_V1,
    RESOURCE_POSTGRESQL_BACKUP,
    backoff=operator_config.BOOTSTRAP_RETRY_DELAY,
    initial_delay=30,
    when=lambda spec, **_: spec.get(SPEC_BACKUP, {}).get(SPEC_BACKUPTOSS3, {
    }).get(SPEC_BACKUP_BACKUPTOS3_KIND, None) ==
    SPEC_BACKUP_BACKUPTOS3_KIND_SCHEDULE,
)
def backup_daemon(
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
            daemon_backup(meta, spec, patch, status, logger, scheduler)
            stopped.wait(60)
    except (ValueError, Exception):
        traceback.print_exc()
        traceback.format_exc()
    finally:
        logger.warning(
            f"backup_daemon with name: {meta['name']}, namespace: {meta['namespace']}, spec: {spec} are done. remove all jobs and shutdown scheduler."
        )
        scheduler.remove_all_jobs()
        scheduler.shutdown()
        set_cluster_status(meta,
                           SPEC_BACKUP_BACKUP_NEXT_RUN_TIME,
                           "",
                           logger,
                           plural=RESOURCE_POSTGRESQL_BACKUP)
        raise Exception("backup_daemon completed.")
