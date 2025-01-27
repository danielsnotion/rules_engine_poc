import json
import pandas as pd

from py_components.DataPipeline import DataPipeline
from util.PipelineCoverter import create_component

# Example usage with JSON
if __name__ == "__main__":
    # Load pipeline configuration from JSON
    with open("C:\\Users\\lenovo\\PycharmProjects\\rules_engine_poc\\rules_json\\rule1.json", "r") as file:
        config = json.load(file)

    # Initialize pipeline
    pipeline = DataPipeline()

    # Add components from JSON configuration

    for component_config in config["pipeline"]:
        component = create_component(component_config["component"], component_config["params"])

        pipeline.add_component(component)

    # Execute pipeline
    data = {"A": [1, 2, 3, 4, 5], "B": [5, 4, 3, 2, 1], "C": ["X", "Y", "X", "Y", "X"]}
    df = pd.DataFrame(data)
    result = pipeline.execute(df)

    print("Pipeline Result:\n", result)
