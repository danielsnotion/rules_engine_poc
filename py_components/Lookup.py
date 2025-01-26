from py_components.Component import Component


# Lookup Component
class Lookup(Component):
    """
    This function performs a lookup operation by matching the source_column in the input_df
    with the lookup_column in the lookup_df and returns the corresponding value from the return_column.

    Parameters:
    - lookup_df (pandas.DataFrame): The lookup dataframe with the mapping.
    - source_column (str): The column in input_df to be matched with lookup_column.
    - lookup_column (str): The column in lookup_df to be matched with source_column.
    - return_column (str): The column in lookup_df from which to return the value.

    Returns:
    - pandas.Series: A series of values from the return_column in the lookup_df corresponding to the source_column in the input_df.
    """

    def __init__(self, lookup_df, source_column, lookup_column, return_column):
        self.lookup_df = lookup_df
        self.source_column = source_column
        self.lookup_column = lookup_column
        self.return_column = return_column

    def execute(self, df):
        # Perform the lookup by merging input_df with lookup_df on the source_column and lookup_column
        result_df = df.merge(self.lookup_df[[self.lookup_column, self.return_column]],
                                        left_on=self.source_column,
                                        right_on=self.lookup_column,
                                        how='left')

        # Return the values from the return_column after the merge
        return result_df[self.return_column]
