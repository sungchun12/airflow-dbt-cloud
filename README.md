# airflow-dbt-cloud

Examples of scheduling dbt Cloud pipelines in airflow. This is intended to be the simplest and fastest way to get locally started.

## Initial Setup

1. Clone this repo: `git clone https://github.com/sungchun12/airflow-dbt-cloud.git`
2. Download the astro cli: [here](https://github.com/astronomer/astro-cli). Example: `brew install astronomer/tap/astro`
3. For detailed instructions, follow the steps: [here](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart)(optional)

## Usage

Create a dbt Cloud job: [here](https://docs.getdbt.com/docs/dbt-cloud/cloud-quickstart/#create-a-new-job)

Download and turn on docker desktop: [here](https://docs.docker.com/desktop/mac/install/)

Run the following command to start a local airflow deployment:

```bash
astro dev start
# OUTPUT:
# Env file ".env" found. Loading...
# Sending build context to Docker daemon  31.23kB
# Step 1/1 : FROM quay.io/astronomer/ap-airflow:2.2.2-onbuild
# # Executing 7 build triggers
#  ---> Using cache
#  ---> Using cache
#  ---> Using cache
#  ---> Using cache
#  ---> Using cache
#  ---> Using cache
#  ---> Using cache
#  ---> 88792797564d
# Successfully built 88792797564d
# Successfully tagged astro-demo_e3fe3c/airflow:latest
# Airflow Webserver: http://localhost:8080
# Postgres Database: localhost:5432/postgres
# The default credentials are admin:admin

```

Create a dbt Cloud API token:
![image](/images/dbt_cloud_api_token.png)

Add your dbt Cloud API token as an encrypted variable:
![image](/images/airflow_api_token_variable.png)

Turn on the DAG:
![image](/images/turn_on_dag.png)

Add your job config details to the python file: [dbt_cloud_example.py](/dags/dbt_cloud_example.py)

```python
# TODO: MANUALLY create a dbt Cloud job: https://docs.getdbt.com/docs/dbt-cloud/cloud-quickstart#create-a-new-job
# Example dbt Cloud job URL
# https://cloud.getdbt.com/#/accounts/4238/projects/12220/jobs/12389/
# example dbt Cloud job config
dbt_cloud_job_runner_config = dbt_cloud_job_runner(
    account_id=16173, project_id=36467, job_id=30605, cause=dag_file_name
)

```

Verify the job succeeded after running:
![image](/images/verify_job_success.png)
![image](/images/verify_dbt_cloud_job_success.png)

Run the following command to stop a local airflow deployment:

```bash
astro dev stop

# verify that the airflow deployment is stopped
astro dev ps
# Name                            State                           Ports
# astrodemoe3fe3c_postgres_1      Exited (0) 20 seconds ago
# astrodemoe3fe3c_scheduler_1     Exited (0) 19 seconds ago
# astrodemoe3fe3c_webserver_1     Exited (0) 19 seconds ago
```
