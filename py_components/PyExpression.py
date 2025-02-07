import pandas as pd
import logging

from py_components.interface.Component import Component, df_storage

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class PyExpression(Component):
    def __init__(self, step_name, component_type, params):
        super().__init__(step_name, component_type, params)

    def validate_syntax(self, expression):
        """
        Validates if the given Python expression has valid syntax.
        Returns True if syntax is correct, otherwise False.
        """
        try:
            compile(expression, "<string>", "exec")  # Check for syntax errors
            return True
        except SyntaxError as e:
            logging.error(f"Syntax error in expression: {e}")
            return False

    def get_final_expression(self, expression):
        return f"""
input_df = df_storage['{self.params.get('input_df')}']
{expression}
df_storage['{self.params.get('output_df_name')}'] = input_df
"""

    def execute(self):
        """Executes the transformation expressions on the DataFrame."""
        try:
            # Validate required parameters
            input_df_name = self.params.get('input_df')
            expression = self.params.get('expression')
            output_df_name = self.params.get('output_df_name')

            if not input_df_name:
                logging.error("Missing required parameter: 'input_df'")
                return

            if not expression:
                logging.error("Missing required parameter: 'expression'")
                return

            if not output_df_name:
                logging.error("Missing required parameter: 'output_df_name'")
                return

            # Validate input DataFrame existence
            input_df = df_storage.get(input_df_name)
            if input_df is None:
                logging.error(f"Input DataFrame '{input_df_name}' not found in storage.")
                return

            # Validate syntax before execution
            if not self.validate_syntax(expression):
                logging.error("Execution stopped due to syntax error.")
                return

            final_expression = self.get_final_expression(expression)
            logging.info(f"Executing expression:\n{final_expression}")

            # Execute the expression safely within a defined scope
            local_scope = {"df_storage": df_storage, "pd": pd}
            exec(final_expression, {}, local_scope)

        except Exception as e:
            logging.error(f"Error executing PyExpression component: {e}", exc_info=True)


if __name__ == "__main__":
    import pandas as pd

    # Sample DataFrame
    data = {
        'price': [10, 20, 30, 40],
        'quantity': [1, 2, 3, 4],
        'category': ['A', 'B', 'C', 'D'],
        'date': pd.to_datetime(['2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01'])
    }

    df_storage['input_df1'] = pd.DataFrame(data)

    expressions = """
input_df["addition_1"] =  input_df["price"] + input_df["quantity"] + 100
input_df["aggregation_1"] =  input_df["price"].sum() + (25 * 100)
input_df["aggregation_2"] =  input_df["quantity"].mean() * (10 / 100)
input_df["date_1"] =  input_df["date"].dt.year + 1
input_df["string_concat_1"] =  input_df["category"] + " hardcoded string to concat"
input_df["string_concat_2"] =  input_df["category"] + "_" + "hardcoded string to concat"
input_df["addition_2"] =  input_df["price"] + input_df["quantity"]
input_df["aggregation_3"] =  input_df["price"].sum()
input_df["aggregation_4"] =  input_df["quantity"].mean()
input_df["date_2"] =  input_df["date"].dt.year
input_df["string_concat_3"] =  input_df["category"] + input_df["price"].astype(str)
input_df["string_concat_4"] =  input_df["category"] + "_" + input_df["price"].astype(str)
"""

    params = {
        "input_df": "input_df1",
        "expression": expressions,
        "output_df_name": "expression_output"
    }

    expression_component = PyExpression("Python_Expression_Step", "expression", params)
    expression_component.execute()

    output_df = df_storage.get(params["output_df_name"])
    if output_df is not None:
        print(output_df)
