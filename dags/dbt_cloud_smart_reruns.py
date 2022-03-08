# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from datetime import datetime

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.providers.dbt.cloud.operators.dbt import (
    DbtCloudGetJobRunArtifactOperator,
    DbtCloudRunJobOperator,
)
from run_results_parser import dbt_cloud_job_runner


#TODO: add ability to trigger with config
# To access configuration in your DAG use {{ dag_run.conf }}. As core.dag_run_conf_overrides_params is set to True, so passing any configuration here will override task params which can be accessed via {{ params }}.

with DAG(
    dag_id="dbt_cloud_smart_reruns",
    default_args={"dbt_cloud_conn_id": "dbt_cloud", "account_id": 16173},
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    # [START howto_operator_dbt_cloud_get_artifact]
    get_run_results_artifact = DbtCloudGetJobRunArtifactOperator(
        task_id="get_run_results_artifact", run_id=trigger_job_run.output, path="run_results.json"
    )
    # [END howto_operator_dbt_cloud_get_artifact]
    # TODO: add in a way to read in the run_results.json file and check for errors
    # TODO: add in a task to parse the run_results.json and compile the dbt command with hard coded models(xcom push)
    # TODO: add a task to run the compiled dbt command(via xcom pull)
    # TODO: add in a way to use `dbt build --select result:error+ --defer --state <path of run_results.json>`

    # parse_run_results_to_dbt_command = PythonOperator(
    #     task_id="parse_run_results_to_dbt_command",
    #     python_callable=dbt_cloud_job_runner_config.run_job,
    #     provide_context=True,
    # )

    # TODO: add dbt command steps override config based on parsed command in previous step
    # [START howto_operator_dbt_cloud_run_job]
    trigger_job_run = DbtCloudRunJobOperator(
        task_id="trigger_job_run",
        job_id=65767,
        check_interval=10,
        timeout=300,
    )
    # [END howto_operator_dbt_cloud_run_job]


    begin >> get_run_results_artifact >> end

