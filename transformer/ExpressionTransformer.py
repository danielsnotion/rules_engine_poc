import pandas as pd
from lark import Lark, Transformer, v_args

# Custom Transformer to map parsed rules to actual code (Pandas operations)
class ExpressionTransformer(Transformer):

    @v_args(inline=True)
    def add(self, left, right):
        return left + right

    @v_args(inline=True)
    def sub(self, left, right):
        return left - right

    @v_args(inline=True)
    def mul(self, left, right):
        return left * right

    @v_args(inline=True)
    def div(self, left, right):
        return left / right

    @v_args(inline=True)
    def number(self, num):
        return float(num[0])

    @v_args(inline=True)
    def column(self, col):
        return col[0]  # Column name as a string

    def year(self, column):
        return lambda df: df[column].dt.year

    def month(self, column):
        return lambda df: df[column].dt.month

    def day(self, column):
        return lambda df: df[column].dt.day

    # If needed: add methods for GROUP BY, ORDER BY, etc.