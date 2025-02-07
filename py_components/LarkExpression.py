import logging
import pandas as pd
from lark import Lark, Transformer, v_args, Tree

from py_components.interface.Component import Component, df_storage
from grammar.LarkGrammar import dataframe_op_grammar
from transformer.LarkDataFrameTransformer import LarkDataFrameTransformer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def execute_expression(parser, df, expression):
    """Parses and executes an expression on a given DataFrame."""
    try:
        if not expression:
            logging.warning("Empty expression received.")
            return None

        logging.info(f'Executing Expression: {expression}')
        tree = parser.parse(expression)
        transformer = LarkDataFrameTransformer(df)
        result = transformer.transform(tree)

        if isinstance(result, Tree):
            result = result.children[0]  # Extract function from tree

        if callable(result):
            return result(df)  # Apply function to DataFrame

        logging.warning("The result is not callable.")
        return result

    except Exception as e:
        logging.error(f"Error processing expression '{expression}': {e}", exc_info=True)
        return None


class LarkExpression(Component):
    def __init__(self, step_name, component_type, params):
        super().__init__(step_name, component_type, params)
        self.parser = Lark(dataframe_op_grammar, parser='lalr', lexer='standard')

    def execute(self):
        """Executes the transformation expressions on the DataFrame."""
        try:
            input_df_name = self.params.get('input_df')
            if not input_df_name:
                logging.error("Missing input DataFrame name in parameters.")
                return

            input_df = df_storage.get(input_df_name)
            if input_df is None:
                logging.error(f"Input DataFrame '{input_df_name}' not found in storage.")
                return

            expressions = self.params.get('expression', {})
            if not isinstance(expressions, dict):
                logging.error("Invalid expression format. Expected a dictionary.")
                return

            for column_name, expr in expressions.items():
                logging.info(f"Processing expression for column '{column_name}': {expr}")
                result = execute_expression(self.parser, input_df, expr)
                if result is not None:
                    input_df[column_name] = result
                else:
                    logging.warning(f"Skipping column '{column_name}' due to expression failure.")

            self.save_output(input_df)

        except Exception as e:
            logging.error(f"Error executing LarkExpression component: {e}", exc_info=True)


# Example Usage
if __name__ == "__main__":
    data = {
        'price': [10, 20, 30, 40],
        'quantity': [1, 2, 3, 4],
        'category': ['A', 'B', 'C', 'D'],
        'date': pd.to_datetime(['2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01'])
    }

    df_storage['input_df1'] = pd.DataFrame(data)

    expressions = {
        "addition_1": "price + quantity + 100",
        "aggregation_1": "SUM(price) + (25*100)",
        "aggregation_2": "AVG(quantity) * (10/100)",
        "date_1": "YEAR(date) + 1",
        "string_concat_1": "CONCAT(category, \" hardcoded string to concat\")",
        "string_concat_2": "CONCAT(category, \"_\", \"hardcoded string to concat\")",
        "addition_2": "price + quantity",
        "aggregation_3": "SUM(price)",
        "aggregation_4": "AVG(quantity)",
        "date_2": "YEAR(date)",
        "string_concat_3": "CONCAT(category, price)",
        "string_concat_4": "CONCAT(category, \"_\", price)",
    }

    params = {
        "input_df": "input_df1",
        "expression": expressions,
        "output_df_name": "expression_output"
    }

    expression_component = LarkExpression("Lark_Expression_Step", "expression", params)
    expression_component.execute()

    output_df = df_storage.get(expression_component.output_df_name)
    if output_df is not None:
        print(output_df)
