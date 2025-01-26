import json
import pandas as pd
from py_components.Aggregate import Aggregate
from py_components.DataPipeline import DataPipeline
from py_components.Filter import Filter
from py_components.Intersect import Intersect
from py_components.Lookup import Lookup
from py_components.Minus import Minus
from py_components.StepAggregate import StepAggregate
from py_components.Union import Union


# Function to dynamically create components from JSON
def create_component(component_name, params):
    if component_name == "Filter":
        return Filter(**params)
    elif component_name == "Aggregate":
        params["aggregate_func"] = eval(params["aggregate_func"])  # Convert string to function
        return Aggregate(**params)
    elif component_name == "Lookup":
        lookup_df = pd.DataFrame(params.pop("lookup_data"))
        return Lookup(lookup_df=lookup_df, **params)
    elif component_name == "Union":
        other_df = pd.DataFrame(params.pop("other_data"))
        return Union(other_df=other_df)
    elif component_name == "Minus":
        other_df = pd.DataFrame(params.pop("other_data"))
        return Minus(other_df=other_df)
    elif component_name == "Intersect":
        other_df = pd.DataFrame(params.pop("other_data"))
        return Intersect(other_df=other_df)
    elif component_name == "StepAggregate":
        params["aggregate_func"] = eval(params["aggregate_func"])  # Convert string to function
        return StepAggregate(**params)
    else:
        raise ValueError(f"Unknown component: {component_name}")

# Example usage with JSON
if __name__ == "__main__":
    # Load pipeline configuration from JSON
    with open("C:\\Users\\lenovo\\PycharmProjects\\rules_execution_engine_poc\\rule.json", "r") as file:
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
