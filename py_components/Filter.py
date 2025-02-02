# Filter component
import duckdb
import pandas as pd
import sqlparse

from py_components.interface.Component import Component, df_storage


class Filter(Component):
    def __init__(self, step_name, component_type, params):
        super().__init__(step_name, component_type, params)

    def validate_condition(self, con, df_name, condition):
        """
        Validate SQL WHERE condition syntax.

        :param df_name: Name of the DataFrame
        :param condition: SQL WHERE condition as string
        :return: True if valid, else raises ValueError

        """
        try:
            parsed = sqlparse.parse(f"SELECT * FROM {df_name} WHERE {condition}")
            if not parsed:
                raise ValueError("Invalid SQL syntax in condition")
            con.execute(f"EXPLAIN SELECT * FROM {df_name} WHERE {condition}")
            return True
        except Exception as e:
            raise ValueError(f"SQL Condition Validation Failed: {str(e)}")

    def execute(self):
        input_df = df_storage.get(self.params['input_df'])
        condition = self.params.get('condition')

        if not condition:
            raise ValueError("Condition parameter is required")
        # Use DuckDB to filter DataFrame
        con = duckdb.connect()
        con.register("input_df", input_df)

        try:
            # Register the original DataFrames
            for name, df in df_storage.items():
                con.register(name, df)
            # Validate SQL condition syntax
            self.validate_condition(con, self.params['input_df'], condition)

            query = f"SELECT * FROM input_df WHERE {condition}"
            result_df = con.execute(query).fetchdf()
            con.close()
        except Exception as e:
            con.close()
            raise ValueError(f"Filter Execution Failed: {str(e)}")

        self.save_output(result_df)


if __name__ == '__main__':
    # Test Filter component

    df_storage['input_df'] = pd.DataFrame({
        "A": [1, 2, 3, 4, 5],
        "B": [10, 20, 30, 40, 50]
    })

    print(f"Input DataFrame:{df_storage.get('input_df')}")
    print("Filtering DataFrame where A > 2")

    filter_component = Filter("Filter", "filter", {
        "input_df": "input_df",
        "condition": "A > 2",
        "output_df_name": "filtered_df"
    })

    filter_component.execute()
    output_df = df_storage.get(filter_component.output_df_name)
    print(output_df)
