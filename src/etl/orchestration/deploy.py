import os

from prefect.deployments import Deployment

from src.etl.orchestration.flows import run_repo_content_flow, run_repo_info_flow
from src.utils.logger import logger


def main(queue=None, pool=None, version: int = 1):
    queue = queue or os.getenv("PREFECT_WORK_QUEUE")
    pool = pool or os.getenv("PREFECT_WORK_POOL")

    deployment = Deployment.build_from_flow(
        flow=run_repo_info_flow,
        name="elt-repo-info-deployment",
        version=version,
        work_queue_name=queue,
        work_pool_name=pool,
    )
    deployment.apply()

    deployment = Deployment.build_from_flow(
        flow=run_repo_content_flow,
        name="elt-repo-content-deployment",
        version=version,
        work_queue_name=queue,
        work_pool_name=pool,
    )
    deployment.apply()


if __name__ == "__main__":
    logger.info("🕐 Starting deployment...")
    main()
    logger.info("✅ Flows have been deployed!")
