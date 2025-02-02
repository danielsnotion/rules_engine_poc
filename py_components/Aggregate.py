# Aggregate component
import pandas as pd

from py_components.interface.Component import Component, df_storage


class Aggregate(Component):
    def __init__(self, step_name, component_type, params):
        super().__init__(step_name, component_type, params)

    def execute(self):
        input_df = df_storage.get(self.params['input_df'])
        group_by = self.params.get('group_by')
        aggregations = self.params.get('aggregations', {})

        if not group_by or not aggregations:
            raise ValueError("Both 'group_by' and 'aggregations' parameters are required")

        # Build aggregation dictionary with default alias handling
        agg_dict = {}
        rename_dict = {}

        for column, agg_funcs in aggregations.items():
            if isinstance(agg_funcs, dict):  # If aliases are provided
                for agg_func, alias in agg_funcs.items():
                    agg_dict[column] = agg_dict.get(column, []) + [agg_func]
                    rename_dict[f"{agg_func}_{column}"] = alias if alias else f"{agg_func}_{column}"
            else:  # No alias provided, only function names
                for agg_func in agg_funcs:
                    agg_dict[column] = agg_dict.get(column, []) + [agg_func]

        # Perform aggregation
        output_df = input_df.groupby(group_by).agg(agg_dict)

        # Flatten multi-level column names and apply renaming
        output_df.columns = [
            rename_dict.get(f"{agg_func}_{col}", f"{agg_func}_{col}")
            for col, agg_func in output_df.columns
        ]
        output_df.reset_index(inplace=True)
        self.save_output(output_df)


def with_alias():
    params = {
        "input_df": "df1",
        "group_by": ["category"],
        "aggregations": {
            "sales": {"sum": "total_sales", "mean": "average_sales"},
            "quantity": {"max": "max_quantity", "min": ""}  # Empty alias → default name
        },
        "output_df_name": "agg_with_alias_output"
    }

    agg_component = Aggregate("aggregate_step", "aggregate", params)
    agg_component.execute()


def without_alias():
    params = {
        "input_df": "df1",
        "group_by": ["category"],
        "aggregations": {
            "sales": ["sum", "mean"],
            "quantity": ["max", "min"]
        },
        "output_df_name": "agg_without_alias_output"
    }

    agg_component = Aggregate("aggregate_step", "aggregate", params)
    agg_component.execute()


if __name__ == '__main__':
    data = {
        "category": ["A", "A", "B", "B", "C"],
        "sales": [100, 200, 300, 400, 500],
        "quantity": [2, 3, 5, 8, 13]
    }

    df1 = pd.DataFrame(data)
    df_storage = {"df1": df1}  # Simulating stored DataFrame

    print(df1)
    without_alias()
    print(df_storage.get("agg_with_alias_output"))
