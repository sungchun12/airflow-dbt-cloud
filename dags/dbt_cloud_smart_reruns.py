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
from dataclasses import dataclass

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from airflow.providers.dbt.cloud.operators.dbt import (
    DbtCloudGetJobRunArtifactOperator,
    DbtCloudRunJobOperator,
)
from run_results_parser import dbt_command_run_results_parser


# TODO: manually set these variables in the airflow variables UI
@dataclass(frozen=True)  # make attributes immutable
class dbt_cloud_job_rerun_vars:
    """Basic dbt Cloud job rerun configurations."""

    # add type hints
    run_id: int = Variable.get("run_id")  # 46948860
    status_set: set = Variable.get("status_set")  # {'error','fail','warn'}
    dbt_command_override: str = Variable.get("dbt_command_override")  # "dbt build"
    run_downstream_nodes: bool = bool(Variable.get("run_downstream_nodes"))  # True


dbt_command_generator = dbt_command_run_results_parser(
    dbt_cloud_job_rerun_vars.status_set,
    dbt_cloud_job_rerun_vars.dbt_command_override,
    dbt_cloud_job_rerun_vars.run_downstream_nodes,
)


with DAG(
    dag_id="dbt_cloud_smart_reruns",
    default_args={"dbt_cloud_conn_id": "dbt_cloud", "account_id": 16173},
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    get_run_results_artifact = DbtCloudGetJobRunArtifactOperator(
        task_id="get_run_results_artifact",
        run_id=dbt_cloud_job_rerun_vars.run_id,
        path="run_results.json",
    )

    parse_run_results_to_dbt_command = PythonOperator(
        task_id="parse_run_results_to_dbt_command",
        python_callable=dbt_command_generator.get_dbt_command_output,
        op_kwargs={"run_results": get_run_results_artifact.output_file_name},
        provide_context=True,
    )

    xcom_pull = "{{ task_instance.xcom_pull(task_ids='parse_run_results_to_dbt_command',key='return_value') }}"
    trigger_job_smart_rerun = DbtCloudRunJobOperator(
        task_id="trigger_job_smart_rerun",
        steps_override=[xcom_pull],
        job_id=65767,
        check_interval=10,
        timeout=300,
    )

    (
        begin
        >> get_run_results_artifact
        >> parse_run_results_to_dbt_command
        >> trigger_job_smart_rerun
        >> end
    )
