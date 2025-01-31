# Example Usage
import pandas as pd
from py_components.interface.Component import df_storage
from util.DataPipeline import DataPipeline

# Assuming input_df and lookup_df are already defined DataFrames
input_df = pd.DataFrame({
    'col1': ['A', 'B', 'C', 'A', 'B'],
    'col2': [1, 2, 3, 4, 5]
})

lookup_df = pd.DataFrame({
    'col2': [1, 2, 3],
    'col3': ['X', 'Y', 'Z']
})

# Store input DataFrame in the global dictionary
df_storage['input_df'] = input_df
df_storage['lookup_df'] = lookup_df

# Steps in JSON format
steps_json = {
  "pipeline_name": "rule_pipeline",
  "pipeline": [
    {
      "step_name": "filter_step",
      "component_type": "filter",
      "params": {
        "input_df": "input_df",
        "column": "col1",
        "operator": "is not null",
        "output_df_name": "filtered_df"
      }
    },
    {
      "step_name": "aggregate_step",
      "component_type": "aggregate",
      "params": {
        "input_df": "filtered_df",
        "group_by": "col1",
        "agg_func": "sum",
        "output_df_name": "aggregated_df"
      }
    },
    {
      "step_name": "lookup_step",
      "component_type": "lookup",
      "params": {
        "input_df": "aggregated_df",
        "lookup_df": "lookup_df",
        "on": "col2",
        "output_df_name": "lookup_df"
      }
    }
  ]
}


# Initialize and execute the pipeline
pipeline = DataPipeline(steps_json,True)
pipeline.execute()

# Retrieve the final output from the global dictionary
print('===================FINAL RESULT===================')
final_result = pipeline.get_result('aggregated_df')
print(final_result)