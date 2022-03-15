"""Utilities to parse the results of the run_results.json"""

import json


class dbt_command_run_results_parser:
    def __init__(self, status_set, dbt_command_override, run_downstream_nodes):
        self.status_set = status_set # ex: {'warn', 'error', 'fail'}
        self.dbt_command_override = dbt_command_override 
        self.run_downstream_nodes = run_downstream_nodes

    def get_dbt_command_output(self, run_results) -> str:
        run_results_dict = self.get_run_results_to_dict(run_results)
        filtered_run_results_set = self.filter_run_results_dict_by_status(run_results_dict)
        dbt_command_output = self.parse_run_results_to_dbt_command(filtered_run_results_set)
        return dbt_command_output

    def get_run_results_to_dict(self, run_results) -> dict:
        with open(run_results) as f:
            run_results_dict = json.load(f)
        return run_results_dict
    
    def filter_run_results_dict_by_status(self, run_results_dict) -> set:
        filtered_run_results_set = set()
        run_results_models = run_results_dict.get('results')
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
        if self.run_downstream_nodes == True:
            for model in filtered_run_results_set:
                dbt_command_output += model + '+ '
        else:
            for model in filtered_run_results_set:
                dbt_command_output += model + ' '
        return dbt_command_output
