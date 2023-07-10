import kopf
import logging
import gc

from pgsqlcommons.constants import (
    POSTGRESQL,
    READWRITEINSTANCE,
)
import pgsqlclusters.utiles as pgsql_util


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
    readwrite_conns = pgsql_util.connections(
        spec, meta, patch, pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE),
        False, None, logger, None, status, False)
    output = pgsql_util.exec_command(
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

    conns = pgsql_util.connections_target(
        meta, spec, patch, status, logger,
        pgsql_util.get_field(POSTGRESQL, READWRITEINSTANCE), None,
        [10000, 10001])
    output = pgsql_util.exec_command(conns.get_conns()[0], ["test"],
                                     logger,
                                     interrupt=True)
    logger.warning(f"test test_free_conn end and output = {output}.")
