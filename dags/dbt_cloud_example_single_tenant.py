from airflow import DAG
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from dbt_cloud_utils import dbt_cloud_job_runner


dag_file_name = __file__

# TODO: MANUALLY create a dbt Cloud job: https://docs.getdbt.com/docs/dbt-cloud/cloud-quickstart#create-a-new-job
# Example single tenant dbt Cloud job URL
# https://staging.singletenant.getdbt.com/#/accounts/1/projects/195/jobs/35/
# example dbt Cloud job config
dbt_cloud_job_runner_config = dbt_cloud_job_runner(
    account_id=1, project_id=195, job_id=35, tenant="staging.singletenant", cause=dag_file_name
)


default_args = {
    "owner": "airflow",
    "email": ["airflow@example.com"],
    "depends_on_past": False,
    "start_date": datetime(2001, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "priority_weight": 1000,
}


with DAG(
    "dbt_cloud_example_single_tenant", default_args=default_args, schedule_interval="@once"
) as dag:
    # have a separate extract and load process(think: FivetranOperator and/or custom gcs load to bigquery tasks)
    extract = DummyOperator(task_id="extract")
    load = DummyOperator(task_id="load")
    ml_training = DummyOperator(task_id="ml_training")

    # Single task to execute dbt Cloud job and track status over time
    transform = PythonOperator(
        task_id="run-dbt-cloud-job",
        python_callable=dbt_cloud_job_runner_config.run_job,
        provide_context=True,
    )

    extract >> load >> transform >> ml_training
