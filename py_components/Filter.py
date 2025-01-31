# Filter component
from py_components.interface.Component import Component, df_storage


class Filter(Component):
    def __init__(self, step_name, component_type, params):
        super().__init__(step_name, component_type, params)

    def execute(self):
        input_df = df_storage.get(self.params['input_df'])
        column = self.params['column']
        operator = self.params['operator']
        value = self.params.get('value', None)

        # Handle filtering based on operator
        if operator == '=':
            output_df = input_df[input_df[column] == value]
        elif operator == '!=':
            output_df = input_df[input_df[column] != value]
        elif operator == '>':
            output_df = input_df[input_df[column] > value]
        elif operator == '<':
            output_df = input_df[input_df[column] < value]
        elif operator == '>=':
            output_df = input_df[input_df[column] >= value]
        elif operator == '<=':
            output_df = input_df[input_df[column] <= value]
        elif operator == 'is null':
            output_df = input_df[input_df[column].isnull()]
        elif operator == 'is not null':
            output_df = input_df[input_df[column].notnull()]
        else:
            raise ValueError(f"Unsupported operator: {operator}")
        self.save_output(output_df)
