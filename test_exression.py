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

            # Try parsing the SQL syntax using DuckDB's EXPLAIN without checking table existence
            db_con.execute(f"EXPLAIN {sql}")
        except duckdb.Error as e:
            if "Table with name " in str(e) and "does not exist!" in str(e):
                continue  # Skip table existence errors
            errors.append(f"Query {i + 1} has an error: {str(e)}\nSQL: {sql}")

    return (False, errors) if errors else (True, "All SQL queries are valid")


def execute_multiple_sql_on_dataframe(queries_dict):
    """
    Execute multiple SQL queries on Pandas DataFrames.
    :param queries_dict: Dictionary where keys are variable names, and values are SQL query strings
    :return: The final resulting DataFrame
    """
    db_con = duckdb.connect()

    # Validate all queries before execution
    errors = {var_name: validate_sql(db_con, sql)[1] for var_name, sql in queries_dict.items()}
    if any(not is_valid for is_valid in errors.values()):
        db_con.close()
        error_messages = {k: v for k, v in errors.items() if isinstance(v, list)}
        raise ValueError(f"SQL Validation Failed:\n{error_messages}")

    # Register original DataFrames in DuckDB
    for name, df in df_storage.items():
        db_con.register(name, df)

    last_result = None

    for var_name, sql in queries_dict.items():
        print(f"Executing SQL for variable {var_name}:\n{sql}")
        try:
            result_df = db_con.execute(sql).fetchdf()  # Convert result to DataFrame
            print(f"Execution Result for {var_name}:\n{result_df}")
            db_con.register(var_name, result_df)  # Store results in DuckDB
        except duckdb.Error as e:
            db_con.close()
            raise RuntimeError(f"Error executing SQL for {var_name}: {e}")

        last_result = result_df  # Keep track of last executed DataFrame

    db_con.close()
    return last_result


class Expression(Component):
    def __init__(self, step_name, component_type, params):
        super().__init__(step_name, component_type, params)

    def execute(self):
        expressions_dict = self.params.get('expressions', {})
        if not expressions_dict:
            raise ValueError("No expressions provided to execute.")

        result_df = execute_multiple_sql_on_dataframe(expressions_dict)
        self.save_output(result_df)


# Example Usage
if __name__ == "__main__":
    # Sample DataFrames
    df_storage['input_df1'] = pd.DataFrame({
        "id": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Charlie", "David"],
        "age": [25, 30, 35, 40]
    })

    df_storage['input_df2'] = pd.DataFrame({
        "id": [1, 2, 3, 4],
        "salary": [50000, 60000, 70000, 80000]
    })

    # Sample SQL Queries
    expressions = {
        "result_df1": "SELECT * FROM input_df1 WHERE age > 30;",
        "result_df2": "SELECT input_df1.name, input_df2.salary FROM input_df1 JOIN input_df2 ON input_df1.id = input_df2.id;",
        "result_df3": "SELECT name, salary FROM result_df2 WHERE salary > 60000;"
    }

    # Create and execute Expression component
    expression_component = Expression("expr1", "expression", {"expressions": expressions})
    expression_component.execute()

    # Retrieve the final output DataFrame
    output_df = df_storage.get(expression_component.output_df_name, None)
    print(output_df)