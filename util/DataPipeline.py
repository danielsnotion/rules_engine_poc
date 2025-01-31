# DataPipeline class to process steps from JSON
from py_components.Aggregate import Aggregate
from py_components.Filter import Filter
from py_components.Lookup import Lookup
from py_components.Union import Union
from py_components.interface.Component import df_storage


class DataPipeline:
    def __init__(self, steps_json, debug=False):
        self.steps_json = steps_json
        self.components = self.initialize_components()
        self.debug = debug

    def initialize_components(self):
        # Mapping component type to their corresponding classes
        component_classes = {
            'filter': Filter,
            'aggregate': Aggregate,
            'lookup': Lookup,
            'union': Union
        }

        components = []
        print(f'Executing pipeline : {self.steps_json["pipeline_name"]}')
        for step in self.steps_json['pipeline']:
            step_type = step['component_type']
            params = step['params']
            step_name = step['step_name']

            # Use the mapping to find the corresponding class and instantiate it
            component_class = component_classes.get(step_type)
            if not component_class:
                raise ValueError(f"Unknown component type: {step_type}")

            component = component_class(step_name, step_type, params)
            components.append(component)

        return components

    def execute(self):
        # Execute each component step by step
        for component in self.components:
            component.execute()
            self.debug_print(component)

    def debug_print(self, component):
        if self.debug:
            print(f"Step: {component.step_name}")
            print(
                f"Input DataFrame ({component.params.get('input_df', '')}):\n{df_storage.get(component.params.get('input_df', ''))}\n")
            print(f"Output DataFrame ({component.output_df_name}):\n{df_storage.get(component.output_df_name)}\n")

    def get_result(self, output_df_name):
        return df_storage.get(output_df_name)
