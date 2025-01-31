# Intersect Component
from py_components.interface.Component import Component


class Intersect(Component):
    """
    Intersect Component to find the intersection of rows between two DataFrames.

    Parameters:
    - other_df (pd.DataFrame): The DataFrame to intersect with.

    Example:
    ```python
    intersect_component = Intersect(other_df)
    intersected_df = intersect_component.execute(df)
    ```
    """
    def __init__(self, other_df):
        self.other_df = other_df

    def execute(self, df):
        return df[df.isin(self.other_df.to_dict(orient="list")).all(axis=1)]