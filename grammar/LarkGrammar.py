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