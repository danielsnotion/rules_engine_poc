import pandas as pd

from py_components.interface.Component import Component, df_storage


class Union(Component):
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
            input_dfs = [df_storage.get(df_key) for df_key in self.params['input_df']]

            if not all(isinstance(df, pd.DataFrame) for df in input_dfs):
                raise TypeError("All inputs must be pandas DataFrames.")

            output_df = pd.concat(input_dfs, ignore_index=True)
            self.save_output(output_df)
        except KeyError as e:
            raise KeyError(f"Missing key in params: {e}")
        except Exception as e:
            raise RuntimeError(f"An error occurred during execution: {str(e)}")


# Example Usage
if __name__ == "__main__":
    data1 = {"A": [1, 2], "B": [3, 4]}
    data2 = {"A": [5, 6], "B": [7, 8]}
    data3 = {"A": [9, 10], "B": [11, 12]}

    df1 = pd.DataFrame(data1)
    df2 = pd.DataFrame(data2)
    df3 = pd.DataFrame(data3)

    df_storage['input_df1'] = pd.DataFrame(df1)
    df_storage['input_df2'] = pd.DataFrame(df2)
    df_storage['input_df3'] = pd.DataFrame(df3)

    params = {

        "input_df": ["input_df1", "input_df2", "input_df3"],
        "output_df_name": "union_output"
    }
    union_component = Union("union_step", "union", params)
    union_component.execute()

    print("Resultant DataFrame:")
    print(df_storage.get(union_component.output_df_name))
