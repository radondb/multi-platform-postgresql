import logging

import kopf

from config import operator_config
from constants import (
    API_GROUP,
    API_VERSION_V1,
    RESOURCE_POSTGRESQL,
)
from handle import create_cluster, delete_cluster, timer_cluster, update_cluster


@kopf.on.startup()
async def startup(settings: kopf.OperatorSettings, **_kwargs):
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
async def cluster_create(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    **_kwargs,
):
    await create_cluster(meta, spec, patch, status, logger)


@kopf.on.update(API_GROUP,
                API_VERSION_V1,
                RESOURCE_POSTGRESQL,
                timeout=operator_config.BOOTSTRAP_TIMEOUT,
                retries=operator_config.BOOTSTRAP_RETRIES,
                backoff=operator_config.BOOTSTRAP_RETRY_DELAY)
async def cluster_update(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    diff: kopf.Diff,
    **_kwargs,
):
    await update_cluster(meta, spec, patch, status, logger, diff)


@kopf.on.delete(API_GROUP,
                API_VERSION_V1,
                RESOURCE_POSTGRESQL,
                timeout=operator_config.BOOTSTRAP_TIMEOUT,
                retries=operator_config.BOOTSTRAP_RETRIES,
                backoff=operator_config.BOOTSTRAP_RETRY_DELAY)
async def cluster_delete(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    **_kwargs,
):
    await delete_cluster(meta, spec, patch, status, logger)


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
async def cluster_timer(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    **_kwargs,
):
    await timer_cluster(meta, spec, patch, status, logger)
