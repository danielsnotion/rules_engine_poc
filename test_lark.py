import pandas as pd
from lark import Lark, Transformer, v_args, Tree, Token

dataframe_op_grammar = """
    ?start: expr

    ?expr: expr "+" term       -> add
         | expr "-" term       -> sub
         | term

    ?term: term "*" factor     -> mul
         | term "/" factor     -> div
         | factor

    ?factor: NUMBER           -> number
           | COLUMN           -> column
           | STRING           -> string
           | function_call
           | "(" expr ")"

    function_call: date_function
                 | agg_function
                 | str_function

    date_function: "YEAR" "(" COLUMN ")"   -> year
                 | "MONTH" "(" COLUMN ")"  -> month
                 | "DAY" "(" COLUMN ")"    -> day

    agg_function: "SUM" "(" COLUMN ")"     -> sum_agg
                | "AVG" "(" COLUMN ")"     -> mean_agg
                | "MAX" "(" COLUMN ")"     -> max_agg
                | "MIN" "(" COLUMN ")"     -> min_agg
                | "COUNT" "(" COLUMN ")"   -> count_agg

    str_function: "CONCAT" "(" expr ("," expr)* ")" -> concat

    COLUMN: /[a-zA-Z_][a-zA-Z0-9_]*/
    STRING: /"[^"]*"/
    NUMBER: /\\d+(\\.\\d+)?/

    %import common.WS
    %ignore WS
"""


@v_args(inline=True)
class DataFrameTransformer(Transformer):
    def __init__(self, df):
        self.df = df

    def year(self, col):
        return self.df[col].dt.year.astype(int)

    def month(self, col):
        return self.df[col].dt.month

    def day(self, col):
        return self.df[col].dt.day

    def concat(self, *args):
        processed_cols = [self._resolve_operand(arg) for arg in args]

        # Ensure all elements are Pandas Series of the same length
        processed_cols = [
            col.astype(str) if isinstance(col, pd.Series) else pd.Series([str(col)] * len(self.df), index=self.df.index)
            for col in processed_cols
        ]

        # Remove any unnecessary quotes
        processed_cols = [col.str.replace(r'^"|"$', '', regex=True) for col in processed_cols]

        # Use `str.cat()` to concatenate all elements
        return processed_cols[0].str.cat(processed_cols[1:], sep='')

    def sum_agg(self, col):
        return float(self.df[col].sum())

    def mean_agg(self, col):
        return float(self.df[col].mean())

    def max_agg(self, col):
        return float(self.df[col].max())

    def min_agg(self, col):
        return float(self.df[col].min())

    def count_agg(self, col):
        return float(self.df[col].count())

    def add(self, left, right):
        left = self._resolve_operand(left)
        right = self._resolve_operand(right)

        # Handle string concatenation
        if isinstance(left, str) or isinstance(right, str):
            return str(left) + str(right)

        # Handle numeric addition
        if isinstance(left, pd.Series) and isinstance(right, pd.Series):
            return left + right
        elif isinstance(left, pd.Series):
            return left + float(right)
        elif isinstance(right, pd.Series):
            return float(left) + right
        return float(left) + float(right)

    def sub(self, left, right):
        left = self._resolve_operand(left)
        right = self._resolve_operand(right)

        if isinstance(left, pd.Series) and isinstance(right, pd.Series):
            return left - right
        elif isinstance(left, pd.Series):
            return left - float(right)
        elif isinstance(right, pd.Series):
            return float(left) - right
        return float(left) - float(right)

    def mul(self, left, right):
        left = self._resolve_operand(left)
        right = self._resolve_operand(right)

        if isinstance(left, pd.Series) and isinstance(right, pd.Series):
            return left * right
        elif isinstance(left, pd.Series):
            return left * float(right)
        elif isinstance(right, pd.Series):
            return float(left) * right
        return float(left) * float(right)

    def div(self, left, right):
        left = self._resolve_operand(left)
        right = self._resolve_operand(right)

        if isinstance(left, pd.Series) and isinstance(right, pd.Series):
            return left / right
        elif isinstance(left, pd.Series):
            return left / float(right)
        elif isinstance(right, pd.Series):
            return float(left) / right
        return float(left) / float(right)

    def column(self, name):
        return name

    def number(self, value):
        return float(value)

    def _resolve_operand(self, operand):
        if isinstance(operand, Token):
            if operand.type == 'COLUMN':
                return self.df[operand.value]
            elif operand.type == 'NUMBER':
                return float(operand.value)
            elif operand.type == 'STRING':
                return operand.value.strip('"')  # Extract string value properly
        elif isinstance(operand, Tree):
            return operand.children[0]  # Resolve nested expressions
        return operand


# Sample DataFrame
data = {
    'price': [10, 20, 30, 40],
    'quantity': [1, 2, 3, 4],
    'category': ['A', 'B', 'C', 'D'],
    'date': pd.to_datetime(['2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01'])
}
df = pd.DataFrame(data)

# Parser initialization
parser = Lark(dataframe_op_grammar, parser='lalr', lexer='standard')


# Function to execute expression
def execute_expression(df, expr):
    try:
        tree = parser.parse(expr)
        transformer = DataFrameTransformer(df)
        result = transformer.transform(tree)

        if isinstance(result, Tree):
            result = result.children[0]

        return result
    except Exception as e:
        print(f"Error processing expression: {e}")
        return None


# Test expressions
expressions = [
    "price + quantity + 100",
    "SUM(price) + (25*100)",
    "AVG(quantity) * (10/100)",
    "YEAR(date) + 1",
    "CONCAT(category, \" hardcoded string to concat\")",
    "CONCAT(category, \"_\",\"hardcoded string to concat\")",
    "price + quantity",
    "SUM(price)",
    "AVG(quantity)",
    "YEAR(date)",
    "CONCAT(category, price)",
    "CONCAT(category, \"_\", price)",
]

# Run expressions
for expr in expressions:
    print(f"Expression: {expr}")
    result = execute_expression(df, expr)
    print("Result:\n", result, "\n")
