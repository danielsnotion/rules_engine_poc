from py_components.Component import Component

# Union Component
class Union(Component):
    """
    Union Component to combine two pandas DataFrames, removing duplicates.

    Parameters:
    - other_df (pd.DataFrame): The DataFrame to union with.

    Example:
    ```python
    union_component = Union(other_df)
    unioned_df = union_component.execute(df)
    ```
    """
    def __init__(self, other_df):
        self.other_df = other_df

    def execute(self, df):
        return pd.concat([df, self.other_df]).drop_duplicates().reset_index(drop=True)