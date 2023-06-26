import os

from prefect.deployments import Deployment

from src.etl.flows import run_etl
from src.utils.logger import logger


def main(queue=None, pool=None, version: int = 1, name: str = "elt-deployment"):
    queue = queue or os.getenv("PREFECT_WORK_QUEUE")
    pool = pool or os.getenv("PREFECT_WORK_POOL")

    deployment = Deployment.build_from_flow(
        flow=run_etl,
        name=name,
        version=version,
        work_queue_name=queue,
        work_pool_name=pool,
    )
    deployment.apply()


if __name__ == "__main__":
    logger.info("🕐 Starting deployment the flow...")
    main()
    logger.info("✅ Flow has been deployed!")
