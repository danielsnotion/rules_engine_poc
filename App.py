from flask import Flask
from flask_restx import Api
from routes.Router import rules_api as api_namespace

api = Api(
    title='Rule Management API',
    version='1.0',
    description='A simple API to manage rules',
    doc='/doc/'
)

api.add_namespace(api_namespace, '/rules')

# Initialize Flask app
app = Flask(__name__)
app.config['RESTX_MASK_SWAGGER'] = False
api.init_app(app)

if __name__ == '__main__':
    app.run(debug=True)