import pandas as pd
from py_components.interface.Component import Component, df_storage


class Lookup(Component):
    def __init__(self, step_name, component_type, params):
        super().__init__(step_name, component_type, params)

    def execute(self):
        input_df = df_storage.get(self.params['input_df'])
        lookup_df = df_storage.get(self.params['lookup_df'])
        print(f"lookup_df111: {lookup_df}")
        left_on = self.params.get('left_on')
        right_on = self.params.get('right_on')
        on = self.params.get('on')
        selected_columns = self.params.get('columns', [])

        if on:
            merge_kwargs = {'on': on}
        elif left_on and right_on:
            merge_kwargs = {'left_on': left_on, 'right_on': right_on}
        else:
            raise ValueError("Either 'on' or both 'left_on' and 'right_on' must be provided.")

        # Check if the required columns exist in lookup_df
        missing_columns = [col for col in selected_columns if col not in lookup_df.columns]
        if missing_columns:
            raise KeyError(f"The following columns are missing in the lookup dataframe: {missing_columns}")

        merged_df = pd.merge(input_df, lookup_df[selected_columns + ([right_on] if right_on else [on])], how='left',
                             **merge_kwargs)
        merged_df = merged_df[selected_columns]
        self.save_output(merged_df)


if __name__ == '__main__':
    df1 = {"id": [1, 2, 3, 4], "name": ["Alice", "Bob", "Charlie", "David"]}
    df2 = {"id": [1, 2, 3, 4], "age": [25, 30, 35, 40], "salary": [50000, 60000, 70000, 80000]}
    df3 = {"employee_id": [1, 2, 3, 4], "age": [25, 30, 35, 40], "salary": [50000, 60000, 70000, 80000]}

    df_storage['input_df1'] = pd.DataFrame(df1)
    df_storage['input_df2'] = pd.DataFrame(df2)
    df_storage['input_df3'] = pd.DataFrame(df3)

    params = {
        "input_df": "input_df1",
        "lookup_df": "input_df2",
        "on": "id",
        "columns": ["age", "salary"],
        "output_df_name": "lookup_output"
    }

    lkp_1 = Lookup("lookup_step", "lookup", params)
    lkp_1.execute()
    # Get the output DataFrame
    lookup_output = df_storage.get(lkp_1.output_df_name)
    print(lookup_output)
    print('====================================================')
    # Test Lookup component with left_on and right_on
    params2 = {
        "input_df": "input_df1",
        "lookup_df": "input_df3",
        "left_on": "id",
        "right_on": "employee_id",
        "columns": ["salary"],
        "output_df_name": "lookup_output2"
    }

    lkp_2 = Lookup("lookup_step", "lookup", params2)
    lkp_2.execute()
    # Get the output DataFrame
    lookup_output2 = df_storage.get(lkp_2.output_df_name)
    print(lookup_output2)

    # Test Lookup component with left_on and right_on
    params3 = {
        "input_df": "input_df1",
        "lookup_df": "input_df3",
        "left_on": "id",
        "right_on": "employee_id",
        "columns": ["salary", "age"],
        "output_df_name": "lookup_output3"
    }

    lkp_3 = Lookup("lookup_step", "lookup", params3)
    lkp_3.execute()
    # Get the output DataFrame
    lookup_output3 = df_storage.get(lkp_3.output_df_name)
    print(lookup_output3)
