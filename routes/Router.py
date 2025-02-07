import json
import logging

import pandas as pd
from flask import request, jsonify
from flask_restx import Resource, Namespace

from model.RulesTemplate import RulesTemplate
from utils.DataPipeline import DataPipeline
from util.PipelineCoverter import create_component

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
                    config = json.load(file)

                # Initialize pipeline
                pipeline = DataPipeline()

                # Add components from JSON configuration

                for component_config in config["pipeline"]:
                    component = create_component(component_config["component"], component_config["params"])

                    pipeline.add_component(component)

                # Execute pipeline
                data = {"A": [1, 2, 3, 4, 5], "B": [5, 4, 3, 2, 1], "C": ["X", "Y", "X", "Y", "X"]}
                df = pd.DataFrame(data)
                result = pipeline.execute(df)
                logger.info("Rule executed successfully")
                return {"message": "Rule executed successfully", "output": str(result)}, 200
            except Exception as e:
                logger.error("Execution error: %s", str(e))
                return {"message": "Execution error", "error": str(e)}, 500
        except Exception as e:
            logger.error("An error occurred: %s", str(e))
            return {"message": "An error occurred"}, 500
