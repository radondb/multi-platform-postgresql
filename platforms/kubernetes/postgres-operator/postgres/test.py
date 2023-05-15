import kopf
import logging
import gc

from handle import connections, get_field, exec_command, connections_target
from constants import *


def test(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
):

    # test_timeout(meta, spec, patch, status, logger)
    # test_free_conn(meta, spec, patch, status, logger)
    return


def test_timeout(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
):
    # test timeout
    logger.warning(f"start test timeout.")
    readwrite_conns = connections(spec, meta, patch,
                                  get_field(POSTGRESQL, READWRITEINSTANCE),
                                  False, None, logger, None, status, False)
    output = exec_command(
        readwrite_conns.get_conns()[0],
        ["i=1; while true; do echo $i; i=$((i+1)); sleep 1; done"],
        logger,
        interrupt=False,
        timeout=10)
    logger.warning(f"end test and output = {output}")
    readwrite_conns.free_conns()


def test_free_conn(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
):
    # test free_conn
    logger.warning(f"test test_free_conn start.")
    gc.collect()

    conns = connections_target(meta, spec, patch, status, logger,
                               get_field(POSTGRESQL, READWRITEINSTANCE), None,
                               [10000, 10001])
    output = exec_command(conns.get_conns()[0], ["test"],
                          logger,
                          interrupt=True)
    logger.warning(f"test test_free_conn end and output = {output}.")
