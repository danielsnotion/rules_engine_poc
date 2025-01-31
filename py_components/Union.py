from py_components.interface.Component import Component, df_storage
import pandas as pd


# Union Component
class Union(Component):
    def __init__(self, step_name, component_type, params):
        super().__init__(step_name, component_type, params)

    def execute(self):
        input_df = df_storage.get(self.params['input_df'])
        union_df = df_storage.get(self.params['union_df'])
        output_df = pd.concat([input_df, union_df], ignore_index=True)
        self.save_output(output_df)
