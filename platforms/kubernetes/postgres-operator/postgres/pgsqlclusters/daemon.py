import kopf
import logging
import traceback

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from pgsqlbackups.backup import backup_postgresql_by_main_cr
from pgsqlbackups.utils import get_backup_mode
from pgsqlcommons.constants import *
from pgsqlclusters.utiles import set_cluster_status


def cron_backup(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    cron_expression: str,
) -> None:
    try:
        if get_backup_mode(meta, spec, patch, status, logger,
                           [BACKUP_MODE_S3_CRON]) == BACKUP_MODE_S3_CRON:
            backup_postgresql_by_main_cr(meta, spec, patch, status, logger)
    except kopf.PermanentError:
        logger.error(f"cron_backup failed.")
        traceback.print_exc()
        traceback.format_exc()


def cron_cluster(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    scheduler: BackgroundScheduler,
) -> None:

    cron = spec.get(SPEC_BACKUPCLUSTER,
                    {}).get(SPEC_BACKUPTOS3, {}).get(SPEC_BACKUPTOS3_CRON, {})
    new_cron_expression = cron.get(SPEC_BACKUPTOS3_CRON_SCHEDULE, None)
    cron_enable = cron.get(SPEC_BACKUPTOS3_CRON_ENABLE, None)
    jobs = scheduler.get_jobs()

    if cron_enable:
        if not jobs:
            logger.info(
                f"current cronjob is empty, add cronjob with {new_cron_expression}"
            )
            job = scheduler.add_job(
                func=cron_backup,
                trigger=CronTrigger.from_crontab(new_cron_expression),
                args=(meta, spec, patch, status, logger, new_cron_expression))
            logger.info(f"add job success.")
        else:
            job = jobs[0]
            old_cron_expression = job.args[-1]
            if old_cron_expression != new_cron_expression:
                # modify job args and reschedule job
                # job.modify(args=(meta, spec, patch, status, logger, new_cron_expression))
                # job = scheduler.reschedule_job(job_id=job.id, jobstore=None,
                #                                trigger=CronTrigger.from_crontab(new_cron_expression))
                job.remove()
                job = scheduler.add_job(
                    func=cron_backup,
                    trigger=CronTrigger.from_crontab(new_cron_expression),
                    args=(meta, spec, patch, status, logger,
                          new_cron_expression))
                logger.info(f"reschedule job success.")
            else:
                logger.info(f"job exists.")
    else:
        # if enable is false but have job, remove job from scheduler
        if jobs:
            logger.warning(
                f"cron not enable, remove job with {jobs[0].args[-1]}")
            jobs[0].remove()

    next_run_time = ""
    if cron_enable:
        next_run_time = str(job.next_run_time).replace(" ", "T")
        logger.info(f"cronjob next run time is {next_run_time}")
    if next_run_time != status.get(CLUSTER_STATUS_CRON_NEXT_RUN, None):
        set_cluster_status(meta, CLUSTER_STATUS_CRON_NEXT_RUN, next_run_time,
                           logger)


def daemon_cluster(
    meta: kopf.Meta,
    spec: kopf.Spec,
    patch: kopf.Patch,
    status: kopf.Status,
    logger: logging.Logger,
    scheduler: BackgroundScheduler,
) -> None:

    cron_cluster(meta, spec, patch, status, logger, scheduler)
