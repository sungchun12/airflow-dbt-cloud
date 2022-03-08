"""Utilities to parse the results of the run_results.json script."""

import json

# read in the json file into a dictionary


# filter the dictionary for models with the relevant status, functions like either/or criteria:pass, warn, error, fail, etc.

# concatenate output string 
# example: dbt build --select abc def xyz
with open("/Users/sung/Desktop/airflow-dbt-cloud/dags/run_results.json") as f:
    run_results = json.load(f)

status_set = {'error','fail','warn'}

dbt_command_override = "dbt build"

run_downstream_nodes = True


# TODO: figure out what data type the get artifact operator outputs: string, json, dict, etc
class dbt_command_run_results_parser:
    def __init__(self, run_results, status_set, dbt_command_override, run_downstream_nodes):
        self.run_results = run_results
        self.status_set = status_set # ex: {'warn', 'error', 'fail'}
        self.dbt_command_override = dbt_command_override 
        self.run_downstream_nodes = run_downstream_nodes

    def get_dbt_command_output(self) -> str:
        filtered_run_results_set = self.filter_run_results_dict_by_status()
        dbt_command_output = self.parse_run_results_to_dbt_command(filtered_run_results_set)
        return dbt_command_output

    def get_run_results_to_dict(self) -> dict:
        run_results_dict = json.load(self.run_results)
        return run_results_dict
    
    def filter_run_results_dict_by_status(self) -> set:
        filtered_run_results_set = set()
        run_results_models = self.run_results.get('results')
        for model in run_results_models:
            if model.get('status') in self.status_set:
                filtered_model_id = model.get('unique_id')
                filtered_model_name = filtered_model_id.split('.')[2]
                filtered_run_results_set.add(filtered_model_name)

        if filtered_run_results_set == set():
            raise Exception(f"No models with status {self.status_set} found in run_results.json")
        else:
            return filtered_run_results_set

    def parse_run_results_to_dbt_command(self, filtered_run_results_set) -> str:
        dbt_command_output = f"{self.dbt_command_override} --select "
        if run_downstream_nodes == True:
            for model in filtered_run_results_set:
                dbt_command_output += model + '+ '
        else:
            for model in filtered_run_results_set:
                dbt_command_output += model + ' '
        return dbt_command_output

dbt_command_output = dbt_command_run_results_parser(run_results,status_set,dbt_command_override,run_downstream_nodes)

print(dbt_command_output.get_dbt_command_output())