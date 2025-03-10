# Example Usage
import json

import pandas as pd

from database.RedisCache import RedisCache
from py_components.interface.Component import df_storage
from utils.DataPipeline import DataPipeline

try:
    redis_cache = RedisCache()

    df = pd.read_csv('C:\\Users\\lenovo\\PycharmProjects\\rules_engine_poc\\sample_data\\Customer-Churn-Records.csv')

    # Retrieve DataFrame
    input_df = redis_cache.get_dataframe("sample_df")

    print("DataFrame retrieved from Redis:", input_df)

    # Store input DataFrame in the global dictionary
    df_storage['input_df'] = input_df
    df_storage['lookup_df'] = input_df

    with open('C:\\Users\\lenovo\\PycharmProjects\\rules_engine_poc\\rules_json\\redis_rule4.json', 'r') as rule_json:
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


except Exception as e:
    print(f"Error: {e}")
finally:
    redis_cache.close_connection()
    print("Redis connection closed.")
