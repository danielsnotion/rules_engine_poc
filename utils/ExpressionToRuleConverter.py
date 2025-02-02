import json
import re

class ExpressionToRuleConverter:
    def __init__(self):
        self.rule = None

    def convert(self, expression):
        self._convert(expression)
        return self.rule

    def _convert(self, expressions):
        lines = expressions.strip().split("\n")
        pipeline_name = lines[0].split(" = ")[1].strip('"')

        pipeline_steps = []

        for line in lines[1:]:
            match = re.match(r'(\w+) = (\w+)\((.*?)\)', line)
            if match:
                output_df_name, component_type, params_str = match.groups()
                params = dict(re.findall(r'(\w+)="(.*?)"', params_str))
                step_name = output_df_name + "_step"
                pipeline_steps.append({
                    "step_name": step_name,
                    "component_type": component_type,
                    "params": params
                })

        self.rule = json.dumps({"pipeline_name": pipeline_name, "pipeline": pipeline_steps}, indent=2)


if __name__ == '__main__':
    expression = """
    pipeline_name = "rule_pipeline"
    filtered_df = filter(input_df="input_df", column="col1", operator="is null", output_df_name="filtered_df")
    aggregated_df = aggregate(input_df="filtered_df", group_by="col1", agg_func="sum", output_df_name="aggregated_df")
    lookup_df = lookup(input_df="aggregated_df", lookup_df="lookup_df", on="col2", output_df_name="lookup_df")
    """
    rules_json = ExpressionToRuleConverter().convert(expression)
    print(rules_json)