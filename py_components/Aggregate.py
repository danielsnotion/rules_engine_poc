# Aggregate component
from py_components.interface.Component import Component, df_storage


class Aggregate(Component):
    def __init__(self, step_name, component_type, params):
        super().__init__(step_name, component_type, params)

    def execute(self):
        input_df = df_storage.get(self.params['input_df'])
        group_by = self.params.get('group_by')
        agg_func = self.params.get('agg_func', 'sum')
        output_df = input_df.groupby(group_by).agg(agg_func)
        self.save_output(output_df)
