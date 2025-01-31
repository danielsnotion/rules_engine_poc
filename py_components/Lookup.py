import pandas as pd
from py_components.interface.Component import Component, df_storage


# Lookup component
class Lookup(Component):
    def __init__(self, step_name, component_type, params):
        super().__init__(step_name, component_type, params)

    def execute(self):
        input_df = df_storage.get(self.params['input_df'])
        lookup_df = df_storage.get(self.params['lookup_df'])
        on = self.params.get('on')
        output_df = pd.merge(input_df, lookup_df, on=on, how='left')
        self.save_output(output_df)
