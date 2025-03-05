import json
import logging

import pandas as pd
from flask import request, jsonify
from flask_restx import Resource, Namespace

from model.RulesTemplate import RulesTemplate
from py_components.interface.Component import df_storage
from utils.DataPipeline import DataPipeline

rules_api = Namespace('Rules Execution Engine', description='API to execute rules')
rules_template = RulesTemplate(rules_api)
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@rules_api.route('/execute')
class RulesExecution(Resource):
    """
    Execute a rule from a Python file
    """

    @rules_api.expect(rules_template.execute_rule_expected_payload())
    @rules_api.marshal_with(rules_template.execute_rule_response())
    def post(self):
        try:

            filename = request.json.get('rule_file')
            if filename == '' or not filename.endswith('.json'):
                logger.error("Invalid file provided: %s", filename)
                return jsonify({"message": "Invalid file"}), 400
            base_path = 'rules_json'
            # Save and execute the file
            file_path = f"{base_path}/{filename}"

            try:
                # Load pipeline configuration from JSON
                with open(file_path, "r") as file:
                    steps_json = json.load(file)

                # Add components from JSON configuration
                # Assuming input_df and lookup_df are already defined DataFrames
                input_df = pd.DataFrame({
                    'col1': ['A', 'B', 'C', 'A', 'B'],
                    'col2': [1, 2, 3, 4, 5]
                })

                lookup_df = pd.DataFrame({
                    'col2_1': [1, 2, 3],
                    'col3': ['X', 'Y', 'Z']
                })

                # Store input DataFrame in the global dictionary
                df_storage['input_df'] = input_df
                df_storage['lookup_df'] = lookup_df

                pipeline = DataPipeline(steps_json, True)
                pipeline.execute()
                final_result = pipeline.get_result('final_output')
                logger.info("Rule executed successfully")
                return {"message": "Rule executed successfully", "output": str(final_result)}, 200
            except Exception as e:
                logger.error("Execution error: %s", str(e))
                return {"message": "Execution error", "error": str(e)}, 500
        except Exception as e:
            logger.error("An error occurred: %s", str(e))
            return {"message": "An error occurred"}, 500
