# Step Aggregate Component
import pandas as pd
from py_components.Component import Component

class StepAggregate(Component):
    """
    Step Aggregate Component to perform aggregation on slices of a DataFrame.

    Parameters:
    - step_size (int): The number of rows in each step.
    - aggregate_func (callable): The aggregation function to apply to each step.

    Example:
    ```python
    step_aggregate_component = StepAggregate(step_size=2, aggregate_func=lambda df: df.sum())
    step_aggregated_df = step_aggregate_component.execute(df)
    ```
    """
    def __init__(self, step_size, aggregate_func):
        self.step_size = step_size
        self.aggregate_func = aggregate_func

    def execute(self, df):
        return pd.concat(
            [self.aggregate_func(df.iloc[i : i + self.step_size]) for i in range(0, len(df), self.step_size)],
            ignore_index=True,
        )