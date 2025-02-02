import pandas as pd
from py_components.interface.Component import Component, df_storage

class Lookup(Component):
    def __init__(self, step_name, component_type, params):
        super().__init__(step_name, component_type, params)

    def execute(self):
        input_df = df_storage.get(self.params['input_df'])
        lookup_df = df_storage.get(self.params['lookup_df'])

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

        self.save_output(merged_df)