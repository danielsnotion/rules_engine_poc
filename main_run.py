import pandas as pd

from py_components.Aggregate import Aggregate
from py_components.DataPipeline import DataPipeline
from py_components.Filter import Filter
from py_components.Lookup import Lookup

# Example Usage
if __name__ == "__main__":
    # Sample DataFrame
    data = {"A": [1, 2, 3, 4, 5], "B": [5, 4, 3, 2, 1], "C": ["X", "Y", "X", "Y", "X"]}
    df = pd.DataFrame(data)

    # Lookup DataFrame
    lookup_data = {"key": [1, 2, 3], "value": ["one", "two", "three"]}
    lookup_df = pd.DataFrame(lookup_data)

    # Sample pipeline setup
    pipeline = DataPipeline()

    # Add components to pipeline
    #pipeline.add_component(Filter("A", "==", 2))  # Keep rows where column A equals 2
    pipeline.add_component(Filter("B", "is not null"))  # Keep rows where column B is not null
    #pipeline.add_component(Lookup(lookup_df,'A','key','value'))
    pipeline.add_component(Aggregate('sum','A',['C'],'test')) # Group by column C and sum

    # Execute pipeline
    result = pipeline.execute(df)
    print("Pipeline Result:\n", result)