from py_components.Component import Component

# Filter Component
class Filter(Component):
    """
    Filter Component to filter rows in a pandas DataFrame based on a column, operator, and value.

    Parameters:
    - column (str): The column name on which to apply the filter.
    - operator (str): The operator to use for filtering (e.g., '==', '!=', '<', '<=', '>', '>=', 'is null', 'is not null').
    - value (any, optional): The value to compare against for filtering. Not needed for 'is null' and 'is not null'.

    Example:
    ```python
    filter_component = Filter("A", "==", 2)
    filtered_df = filter_component.execute(df)
    ```
    """
    def __init__(self, column, operator, value=None):
        self.column = column
        self.operator = operator
        self.value = value

    def execute(self, df):
        if self.operator == "==":
            return df[df[self.column] == self.value]
        elif self.operator == "!=":
            return df[df[self.column] != self.value]
        elif self.operator == "<":
            return df[df[self.column] < self.value]
        elif self.operator == "<=":
            return df[df[self.column] <= self.value]
        elif self.operator == ">":
            return df[df[self.column] > self.value]
        elif self.operator == ">=":
            return df[df[self.column] >= self.value]
        elif self.operator == "is null":
            return df[df[self.column].isnull()]
        elif self.operator == "is not null":
            return df[df[self.column].notnull()]
        else:
            raise ValueError(f"Unsupported operator: {self.operator}")