import os

from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

from src.etl.orchestration.flows import (
    run_etl_flow,
    run_repo_content_flow,
    run_repo_info_flow,
)
from src.utils.api import get_current_date
from src.utils.logger import logger


def build_deployments(queue=None, pool=None, cron_expr="0 0 * * *", window=7, version: int = 1):
    queue = queue or os.getenv("PREFECT_WORK_QUEUE")
    pool = pool or os.getenv("PREFECT_WORK_POOL")
    cron_expr = cron_expr or os.getenv("PREFECT_CRON_EXPRESSION")
    window = window or os.getenv("PREFECT_UPDATE_WINDOW")

    Deployment.build_from_flow(
        flow=run_repo_info_flow,
        name="elt-repo-info-deployment",
        version=version,
        work_queue_name=queue,
        work_pool_name=pool,
        apply=True,
    )

    Deployment.build_from_flow(
        flow=run_repo_content_flow,
        name="elt-repo-content-deployment",
        version=version,
        work_queue_name=queue,
        work_pool_name=pool,
        apply=True,
    )

    Deployment.build_from_flow(
        flow=run_etl_flow,
        name="elt-deployment",
        version=version,
        work_queue_name=queue,
        work_pool_name=pool,
        schedule=(CronSchedule(cron=cron_expr, timezone="UTC")),
        parameters={
            "start_date": get_current_date(shift=window),
            "end_date": get_current_date(shift=1),
            "overwrite_existed_files": True,
        },
        apply=True,
    )


if __name__ == "__main__":
    logger.info("🕐 Starting deployment...")
    build_deployments()
    logger.info("✅ Flows have been deployed!")
