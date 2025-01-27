# Aggregate Component
import pandas as pd

from py_components.Component import Component


class Aggregate(Component):
    """
    This function performs aggregation on a pandas DataFrame.

    Parameters:
    - agg_func (str or function): The aggregation function to apply.
      Can be a string like 'sum', 'mean', 'median', etc., or a custom aggregation function.
    - agg_column (str): The column on which to apply the aggregation.
    - groupby_columns (list of str): A list of columns to group by.
    - output_column (str, optional): The name of the output column. If not provided,
      the column name will follow the pattern aggregate_function_aggregate_columns.

    Returns:
    - pandas.DataFrame: The aggregated dataframe.
    """

    def __init__(self, agg_func, agg_column, group_columns, output_column=None):
        self.agg_func = agg_func
        self.agg_column = agg_column
        self.group_columns = group_columns
        self.output_column = output_column

    def execute(self, df):
        # Check if aggregation function is string or callable, and use accordingly
        if isinstance(self.agg_func, str):
            agg_func = self.agg_func.lower()

        # Perform aggregation
        agg_df = df.groupby(self.group_columns).agg({self.agg_column: self.agg_func}).reset_index()

        # Create output column name if not provided
        if self.output_column is None:
            output_column = f"{self.agg_func}_{self.agg_column}_{'_'.join(self.group_columns)}"

        # Rename the column to match the pattern if necessary
        agg_df.rename(columns={self.agg_column: self.output_column}, inplace=True)

        return agg_df
