# flows/trigger_dbt.py
from prefect import flow, task
import subprocess
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

@task
def run_dbt_transformations():
    logger.info("Starting DBT run...")
    # Build the command. You may use --profiles-dir and --project-dir if needed.
    cmd = ["dbt", "run", "--profiles-dir", "dbt/profiles", "--project-dir", "dbt"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        logger.info("DBT run succeeded.")
    else:
        logger.error(f"DBT run failed: {result.stderr}")
        raise Exception("DBT run failed")
    return result.stdout

@flow(name="dbt_transformations_flow")
def dbt_transformations_flow():
    run_dbt_transformations()

