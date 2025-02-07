import pandas as pd
from lark import Lark, Transformer

# Lark Grammar
grammar = """
    start: expression

    expression: operation (group_by? order_by?)?

    operation: arithmetic
             | aggregation
             | filter

    arithmetic: COLUMN OPERATOR COLUMN          -> binary_op
              | COLUMN OPERATOR NUMBER          -> binary_op
              | "(" arithmetic ")"              -> parens

    aggregation: FUNC "(" COLUMN ")"            -> aggregate_func

    filter: "WHERE" COLUMN COMP NUMBER          -> filter_condition
          | "WHERE" COLUMN COMP COLUMN          -> filter_condition

    group_by: "GROUP BY" COLUMN                 -> group_by_clause

    order_by: "ORDER BY" COLUMN ("ASC"|"DESC")? -> order_by_clause

    FUNC: "SUM" | "AVG" | "COUNT" | "MIN" | "MAX"   // <-- Fixed function token
    OPERATOR: "+" | "-" | "*" | "/"
    COMP: ">" | "<" | ">=" | "<=" | "==" | "!="
    COLUMN: /[a-zA-Z_][a-zA-Z0-9_]*/
    NUMBER: /[0-9]+(\\.[0-9]+)?/

    %import common.WS
    %ignore WS
"""
# Lark Parser
parser = Lark(grammar, start="start", parser="lalr")

# Transformer to Convert AST to Pandas Code
class PandasTransformer(Transformer):
    def binary_op(self, items):
        return f"df['{items[0]}'] {items[1]} df['{items[2]}']"

    def aggregate_func(self, items):
        return f"df.groupby('{items[1]}').agg({{'{items[1]}': '{items[0].lower()}'}})"

    def filter_condition(self, items):
        return f"df[df['{items[0]}'] {items[1]} {items[2]}]"

    def group_by_clause(self, items):
        return f"df.groupby('{items[0]}')"

    def order_by_clause(self, items):
        order = "ascending=True" if len(items) == 1 or items[1] == "ASC" else "ascending=False"
        return f"df.sort_values(by='{items[0]}', {order})"

# Convert Expression to Pandas Code
def convert_expression(expression):
    tree = parser.parse(expression)
    transformer = PandasTransformer()
    return transformer.transform(tree)

# Example DataFrame
df = pd.DataFrame({
    'sales': [100, 200, 300, 400],
    'region': ['North', 'South', 'North', 'South'],
    'price': [10, 20, 30, 40],
    'quantity': [2, 3, 4, 5],
})

# Example Expressions
expressions = [
    "SUM(sales) GROUP BY region",
    "price * quantity",
    "WHERE sales > 100",
    "ORDER BY sales DESC"
]

for expr in expressions:
    print(f"Expression: {expr}")
    pandas_code = convert_expression(expr)
    print(f"Pandas Code: {pandas_code}\n")
