#!/bin/bash

echo "Work pool name: ${PREFECT_WORK_POOL}"
prefect work-pool create "${PREFECT_WORK_POOL}" --type=process

echo "Work queue name: ${PREFECT_WORK_QUEUE}"
prefect work-queue create "${PREFECT_WORK_QUEUE}" --pool="${PREFECT_WORK_POOL}"

python src/etl/orchestration/deploy.py

prefect agent start -p "${PREFECT_WORK_POOL}" -q "${PREFECT_WORK_QUEUE}"