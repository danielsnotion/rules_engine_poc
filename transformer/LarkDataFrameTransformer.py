import pandas as pd
from lark import Transformer, v_args, Tree, Token


@v_args(inline=True)
class LarkDataFrameTransformer(Transformer):
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
