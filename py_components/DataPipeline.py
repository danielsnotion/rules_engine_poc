# Orchestrator
from py_components.Component import Component


class DataPipeline:
    """
    DataPipeline to orchestrate the execution of multiple components on a pandas DataFrame.

    Methods:
    - add_component(component): Adds a component to the pipeline.
    - execute(df): Executes all components in the pipeline sequentially on the given DataFrame.

    Example:
    ```python
    pipeline = DataPipeline()
    pipeline.add_component(Filter("A", "==", 2))
    pipeline.add_component(Aggregate(group_by_columns=["C"], aggregate_func=lambda df: df.sum()))
    result = pipeline.execute(df)
    ```
    """
    def __init__(self):
        self.components = []

    def add_component(self, component):
        if isinstance(component, Component):
            self.components.append(component)
        else:
            raise TypeError("Component must inherit from the Component base class")

    def execute(self, df):
        for component in self.components:
            df = component.execute(df)
        return df