import pandas as pd
from py_components.Aggregate import Aggregate
from py_components.Filter import Filter
from py_components.Intersect import Intersect
from py_components.Lookup import Lookup
from py_components.Minus import Minus
from py_components.StepAggregate import StepAggregate
from py_components.Union import Union

# Function to dynamically create components from JSON
def create_component(component_name, params):
    print(f'component_name: {component_name}, params: {params}')
    if component_name == "Filter":
        return Filter(**params)
    elif component_name == "Aggregate":
        agg_column = params.get("agg_column")
        group_columns = params.get("group_columns")
        agg_func = params.get("agg_func")
        output_column = params.get("output_column")
        return Aggregate(agg_func=agg_func, agg_column=agg_column, group_columns=group_columns, output_column=output_column)
    elif component_name == "Lookup":
        #lookup_df = pd.DataFrame(params.pop("lookup_data"))
        lookup_data = {"key": [1, 2, 3], "value": ["one", "two", "three"]}
        lookup_df = pd.DataFrame(lookup_data)
        source_column = params.get("source_column")
        lookup_column = params.get("lookup_column")
        return_column = params.get("return_column")
        print(f'source_column: {source_column}, lookup_column: {lookup_column}, return_column: {return_column}')
        return Lookup(lookup_df=lookup_df, source_column=source_column,lookup_column=lookup_column,return_column=return_column)
    elif component_name == "Union":
        other_df = pd.DataFrame(params.pop("other_data"))
        return Union(other_df=other_df)
    elif component_name == "Minus":
        other_df = pd.DataFrame(params.pop("other_dataFRYFRY"))
        return Minus(other_df=other_df)
    elif component_name == "Intersect":
        other_df = pd.DataFrame(params.pop("other_data"))
        return Intersect(other_df=other_df)
    elif component_name == "StepAggregate":
        params["aggregate_func"] = eval(params["aggregate_func"])  # Convert string to function
        return StepAggregate(**params)
    else:
        raise ValueError(f"Unknown component: {component_name}")