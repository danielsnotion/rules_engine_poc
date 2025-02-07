# Example Usage
import json

import pandas as pd
from py_components.interface.Component import df_storage
from utils.DataPipeline import DataPipeline

# Assuming input_df and lookup_df are already defined DataFrames
input_df = pd.DataFrame({
    'col1': ['A', 'B', 'C', 'A', 'B'],
    'col2': [1, 2, 3, 4, 5]
})

lookup_df = pd.DataFrame({
    'col2_1': [1, 2, 3],
    'col3': ['X', 'Y', 'Z']
})

# Store input DataFrame in the global dictionary
df_storage['input_df'] = input_df
df_storage['lookup_df'] = lookup_df

with open('C:\\Users\\lenovo\\PycharmProjects\\rules_engine_poc\\rules_json\\rule2.json', 'r') as rule_json:
    rule = json.load(rule_json)

# Steps in JSON format
steps_json = rule

# Initialize and execute the pipeline
pipeline = DataPipeline(steps_json, True)
pipeline.execute()

# Retrieve the final output from the global dictionary
print('===================FINAL RESULT===================')
final_result = pipeline.get_result('final_output')
print(final_result)
