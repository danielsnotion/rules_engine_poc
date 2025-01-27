from flask_restx import fields


class RulesTemplate:
    def __init__(self, namespace):
        self.namespace = namespace

    def execute_rule_expected_payload(self):
        return self.namespace.model('ExecuteRulePayload', {
            'rule_file': fields.String(required=True, description='Rule file path'),
        })
    def execute_rule_response(self):
        return self.namespace.model('ExecuteRuleResponse', {
            'message': fields.String(required=True, description='Rule executed successfully'),
            "output": fields.String(required=False, description='Output of the rule execution'),
            "error": fields.String(required=False, description='Error message if execution fails')
        })