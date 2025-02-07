from py_components.interface.Component import Component, df_storage
import pandas as pd
import sqlparse
import duckdb


def validate_sql(db_con, queries):
    """
    Validate SQL syntax without checking table existence.
    :param db_con: DuckDB connection object
    :param queries: SQL query string with semicolon-separated queries
    :return: Tuple (is_valid, error_message or list of errors)
    """
    sql_statements = [q.strip() for q in queries.split(";") if q.strip()]
    errors = []
    for i, sql in enumerate(sql_statements):
        try:
            parsed = sqlparse.parse(sql)
            if not parsed:
                errors.append(f"Query {i + 1} is invalid: {sql}")

            # Try to parse the SQL syntax without checking table existence
            db_con.execute(f"EXPLAIN {sql}")
        except Exception as e:
            if "Table with name " in str(e) and "does not exist!" in str(e):
                continue  # Skip table existence errors
            errors.append(f"Query {i + 1} has an error: {str(e)}\nSQL: {sql}")

    if errors:
        return False, errors  # Return all errors found

    return True, "All SQL queries are valid"


def execute_multiple_sql_on_dataframe(queries_dict):
    """
    Execute multiple SQL queries on Pandas DataFrames.
    :param queries_dict: Dictionary where keys are variable names, and values are SQL query strings
    :return: The final resulting DataFrame
    """
    db_con = duckdb.connect()

    errors = []
    for var_name, sql in queries_dict.items():
        is_valid, msg = validate_sql(db_con, sql)
        if not is_valid:
            errors.append(msg)

    if errors:
        db_con.close()
        raise ValueError(f"SQL Validation Failed: {errors}")

    # Register the original DataFrames
    for name, df in df_storage.items():
        db_con.register(name, df)

    last_result = None

    # Execute each query in the dictionary
    for var_name, sql in queries_dict.items():
        print(f"Executing SQL for variable {var_name}: {sql}")
        try:
            result_df = db_con.execute(sql).fetchdf()  # Fetch result as DataFrame
            print(result_df)
            db_con.register(var_name, result_df)  # Register the result as a new DataFrame with the given name
        except Exception as e:
            db_con.close()
            raise RuntimeError(f"Error executing SQL for {var_name}: {e}")

        last_result = result_df  # Update last result

    db_con.close()
    return last_result  # Return the final output DataFrame


class SQLExpression(Component):
    def __init__(self, step_name, component_type, params):
        super().__init__(step_name, component_type, params)

    def execute(self):
        expressions_dict = self.params['expressions']  # Dictionary of variable names and SQL queries
        result_df = execute_multiple_sql_on_dataframe(expressions_dict)
        self.save_output(result_df)


# Example Usage
if __name__ == "__main__":
    # Sample DataFrames
    df1 = {"id": [1, 2, 3, 4], "name": ["Alice", "Bob", "Charlie", "David"], "age": [25, 30, 35, 40]}
    df2 = {"id": [1, 2, 3, 4], "salary": [50000, 60000, 70000, 80000]}

    df_storage['input_df1'] = pd.DataFrame(df1)
    df_storage['input_df2'] = pd.DataFrame(df2)

    # Sample Expression Dictionary
    expressions = {
        "result_df1": "SELECT * FROM input_df1 WHERE age > 30;",
        "result_df2": "SELECT input_df1.name, input_df2.salary FROM input_df1 JOIN input_df2 ON input_df1.id = input_df2.id;",
        "result_df3": "SELECT name, salary FROM result_df2 WHERE salary > 60000;"
    }

    # Create and execute Expression component
    expression_component = SQLExpression("expr1", "expression", {"expressions": expressions})
    expression_component.execute()

    # Get the output DataFrame (the last resulting DataFrame)
    output_df = df_storage.get(expression_component.output_df_name)
    print(output_df)
