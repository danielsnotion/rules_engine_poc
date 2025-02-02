from operator import index

import pandas as pd

from py_components.interface.Component import Component, df_storage


class Intersect(Component):
    """
    Intersect Component to find the difference of rows between a primary DataFrame and multiple other DataFrames.
    """

    def __init__(self, step_name, component_type, params):
        super().__init__(step_name, component_type, params)

        if not all(isinstance(df_storage.get(df), pd.DataFrame) for df in params["input_df"]):
            raise TypeError("All provided inputs must be pandas DataFrames.")
        self.dataframes = [df_storage.get(df) for df in params["input_df"]]

        # Ensure all input DataFrames have the same columns
        reference_columns = set(self.dataframes[0].columns)
        for df in self.dataframes[1:]:
            if set(df.columns) != reference_columns:
                raise ValueError("All input DataFrames must have the same columns.")

    def execute(self):
        try:
            df = self.dataframes[0].copy()
            for other_df in self.dataframes[1:]:
                df = pd.merge(df, other_df, how='inner', on=['A', 'B'])

            self.save_output(df)
        except Exception as e:
            raise RuntimeError(f"An error occurred during execution: {str(e)}")


if __name__ == "__main__":
    data1 = {"A": [1, 2, 3, 4], "B": [5, 6, 7, 8]}
    data2 = {"A": [1, 2, 3], "B": [5, 6, 8]}
    data3 = {"A": [1, 2], "B": [5, 6]}

    df1 = pd.DataFrame(data1)
    df2 = pd.DataFrame(data2)
    df3 = pd.DataFrame(data3)
    df_storage['input_df1'] = pd.DataFrame(df1)
    df_storage['input_df2'] = pd.DataFrame(df2)
    df_storage['input_df3'] = pd.DataFrame(df3)

    params = {
        "input_df": ["input_df1", "input_df2", "input_df3"],
        "output_df_name": "intersect_output"
    }
    minus_component = Intersect("intersect_step", "intersect", params)
    output_df = minus_component.execute()

    print("Resultant DataFrame:")
    lookup_output3 = df_storage.get(minus_component.output_df_name)
    print(lookup_output3)
