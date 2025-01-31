# Global dictionary to hold dataframes
df_storage = {}


# Base class for Data Processing Components
class Component:
    def __init__(self, step_name, component_type, params):
        self.step_name = step_name
        self.component_type = component_type
        self.params = params
        self.output_df_name = params.get('output_df_name', step_name)  # Default to step name if not provided

    def execute(self):
        raise NotImplementedError("Subclasses should implement this method")

    def save_output(self, output_df):
        df_storage[self.output_df_name] = output_df
