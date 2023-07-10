import kopf
import logging
import traceback

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from pgsqlcommons.constants import *
from pgsqlbackups.constants import *
from pgsqlbackups.utils import load_and_check_params, check_condition_backup_param, check_condition_pgsql_exists, set_backup_status, set_display_backup_status
from pgsqlbackups.backup import backup_postgresql_by_backup_cr, delete_postgresql_backup, check_backup_valid


def create_backup(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    schedule: str = None,
) -> None:
    if logger is None:
        logger = logging.getLogger("default")
    try:
        set_display_backup_status(meta,
                                  dict(),
                                  logger,
                                  other_status={STATE: STATUS_CREATING})
        state = STATUS_COMPLETED
        backup_params = load_and_check_params(meta, spec, logger,
                                              BACKUP_NEED_PARAMS)
        logger.info(f"backup_params = {backup_params}")

        logger.info(f"create_backup check if backup is possible.")
        pgsql_body = check_condition_pgsql_exists(logger, backup_params)
        if backup_params[SPEC_BACKUP_BACKUPTOS3_KIND] == SPEC_BACKUP_BACKUPTOS3_KIND_SCHEDULE\
                and schedule is None:
            logger.info(f"schedule backup will created by daemon_backup.")
            return
        check_condition_backup_param(pgsql_body[METADATA], pgsql_body[SPEC],
                                     patch, pgsql_body[STATUS], logger,
                                     backup_params)
        logger.info(f"create_backup check backup successful.")

        set_backup_status(pgsql_body[METADATA],
                          STATE_BACK_LOCKING,
                          STATE_BACK_LOCKED,
                          logger,
                          plural=RESOURCE_POSTGRESQL)
        backup_postgresql_by_backup_cr(pgsql_body[METADATA], pgsql_body[SPEC],
                                       patch, status, logger, backup_params)

        # save pgsql body.spec to PostgreSQLBackup CR
        set_backup_status(meta, STATE_SNAPSHOT,
                          {key: pgsql_body[key]
                           for key in REQUIRED_KEYS}, logger)
        set_backup_status(pgsql_body[METADATA],
                          STATE_BACK_LOCKING,
                          STATE_BACK_UNLOCKED,
                          logger,
                          plural=RESOURCE_POSTGRESQL)
    except Exception as e:
        logger.error(f"error occurs, {e}")
        traceback.print_exc()
        traceback.format_exc()
        state = STATUS_CREATE_FAILED
    finally:
        set_backup_status(meta, STATE, state, logger)


def delete_backup(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> None:
    set_backup_status(meta, STATE, STATUS_TERMINATING, logger)
    backup_params = load_and_check_params(meta, spec, logger,
                                          BACKUP_NEED_PARAMS)

    # could not use check_condition_pgsql_exists function (maybe pgsql cluster is deleted)
    snapshot = status.get(STATE_SNAPSHOT, {})
    meta = snapshot.get(METADATA, None)
    spec = snapshot.get(SPEC, None)

    if meta is None or spec is None or 'name' not in meta or SPEC_S3 not in spec:
        logger.warning(f"delete_backup get status.snapshot failed, skip ...")
        return

    delete_postgresql_backup(meta, spec, patch, status, logger, backup_params)


def check_backup(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
) -> None:
    backup_params = load_and_check_params(meta, spec, logger,
                                          BACKUP_NEED_PARAMS)

    snapshot = status.get(STATE_SNAPSHOT, {})
    meta = snapshot.get(METADATA, None)
    spec = snapshot.get(SPEC, None)

    if meta is None or spec is None or 'name' not in meta or SPEC_S3 not in spec:
        logger.warning(f"check_backup get status.snapshot failed, skip ...")
        return
    if status.get(STATE, None) in [STATUS_CREATING, STATUS_TERMINATING]:
        logger.warning(
            f"maybe backup {STATUS_CREATING} or {STATUS_TERMINATING}, skip check_backup ..."
        )
        return

    check_backup_valid(meta, spec, patch, status, logger, backup_params)


def daemon_backup(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    scheduler: BackgroundScheduler,
) -> None:
    backup_params = load_and_check_params(meta, spec, logger,
                                          BACKUP_NEED_PARAMS)
    new_schedule = backup_params[SPEC_BACKUP_BACKUPTOS3_SCHEDULE]
    jobs = scheduler.get_jobs()

    if not jobs:
        logger.info(
            f"current cronjob is empty, add cronjob with {new_schedule}")
        job = scheduler.add_job(func=create_backup,
                                trigger=CronTrigger.from_crontab(new_schedule),
                                args=(meta, spec, patch, status, None,
                                      new_schedule))
        logger.info(f"add job success.")
    else:
        job = jobs[0]
        old_schedule = job.args[-1]
        if old_schedule != new_schedule:
            job.remove()
            job = scheduler.add_job(
                func=create_backup,
                trigger=CronTrigger.from_crontab(new_schedule),
                args=(meta, spec, patch, status, None, new_schedule))
            logger.info(f"reschedule job success.")
        else:
            logger.info(f"job exists.")

    next_run_time = str(job.next_run_time).replace(" ", "T")
    logger.info(f"cronjob next run time is {next_run_time}")
    if next_run_time != status.get(SPEC_BACKUP_BACKUP_NEXT_RUN_TIME, None):
        set_backup_status(meta, SPEC_BACKUP_BACKUP_NEXT_RUN_TIME,
                          next_run_time, logger)
