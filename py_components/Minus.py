# Minus Component
from py_components.interface.Component import Component


class Minus(Component):
    """
    Minus Component to subtract rows of another DataFrame from the main DataFrame.

    Parameters:
    - other_df (pd.DataFrame): The DataFrame whose rows will be removed from the main DataFrame.

    Example:
    ```python
    minus_component = Minus(other_df)
    subtracted_df = minus_component.execute(df)
    ```
    """
    def __init__(self, other_df):
        self.other_df = other_df

    def execute(self, df):
        return df[~df.isin(self.other_df.to_dict(orient="list")).all(axis=1)]