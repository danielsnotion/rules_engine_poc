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


def execute_multiple_sql_on_dataframe(queries):
    """
    Execute multiple SQL queries on Pandas DataFrames.

    :param queries: SQL query string with semicolon-separated queries
    :return: The last resulting DataFrame
    """
    db_con = duckdb.connect()
    is_valid, msg = validate_sql(db_con, queries)
    print(msg)
    if not is_valid:
        db_con.close()
        raise ValueError(f"SQL Validation Failed: {msg}")

    # Split queries by semicolon and remove empty ones
    sql_statements = [q.strip() for q in queries.split(";") if q.strip()]

    # Register the original DataFrames
    for name, df in df_storage.items():
        db_con.register(name, df)

    last_result = None

    for i, sql in enumerate(sql_statements):
        df_version = i + 1
        print(f"Executing SQL {df_version}: {sql}")

        try:
            result_df = db_con.execute(sql).fetchdf()  # Fetch result as DataFrame
            print(result_df)
            db_con.register(f'df_{df_version}', result_df)  # Register the result as a new DataFrame
        except Exception as e:
            db_con.close()
            raise RuntimeError(f"Error executing query {i + 1}: {e}")

        last_result = result_df  # Update last result

    db_con.close()
    return last_result  # Return the final output DataFrame

class Expression(Component):
    def __init__(self, step_name, component_type, params):
        super().__init__(step_name, component_type, params)

    def execute(self):
        expressions1 = self.params['expressions']
        result_df = execute_multiple_sql_on_dataframe(expressions1)
        self.save_output(result_df)


# Example Usage
if __name__ == "__main__":
    # Note ::
    # pip install pandas sqlparse duckdb
    # Sample DataFrames
    df1 = {"id": [1, 2, 3, 4], "name": ["Alice", "Bob", "Charlie", "David"], "age": [25, 30, 35, 40]}
    df2 = {"id": [1, 2, 3, 4], "salary": [50000, 60000, 70000, 80000]}

    df_storage['input_df1'] = pd.DataFrame(df1)
    df_storage['input_df2'] = pd.DataFrame(df2)

    # Sample Expression
    expressions = """
    SELECT * FROM input_df1 WHERE age > 30;
    SELECT input_df1.name, input_df2.salary FROM input_df1 JOIN input_df2 ON input_df1.id = input_df2.id;
    SELECT name, salary FROM df_2 WHERE salary > 60000;
    """

    # Create and execute Expression component
    expression_component = Expression("expr1", "expression",
                                      {"input_df": ["input_df1", "input_df2"], "expressions": expressions})
    expression_component.execute()

    # Get the output DataFrame
    output_df = df_storage.get(expression_component.output_df_name)
    print(output_df)
