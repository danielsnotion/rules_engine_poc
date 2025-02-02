import json


class RuleToExpressionConverter:
    def __init__(self, rule):
        self.rule = rule

    def convert(self):
        return self._convert(self.rule)

    def _convert(self, rule_json):
        expressions = []
        expressions.append(f'pipeline_name = "{rule_json["pipeline_name"]}"\n')

        for component in rule_json["pipeline"]:
            func_name = component["component_type"]
            params = ", ".join(f'{k}="{v}"' for k, v in component["params"].items())
            expr = f'{component["params"]["output_df_name"]} = {func_name}({params})'
            expressions.append(expr)

        return "\n".join(expressions)

if __name__ == '__main__':
    # Open and read the JSON file
    with open('C:\\Users\\lenovo\\PycharmProjects\\rules_engine_poc\\rules_json\\rule.json', 'r') as file:
        rule = json.load(file)

    converter = RuleToExpressionConverter(rule)
    print(converter.convert())