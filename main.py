from flask import Flask
from flask_cors import CORS
from flask import request
import operators.operator
from operators.spark.spark_flow import SparkFlow
from flow_manager import flowManager
app = Flask(__name__)
CORS(app, support_credentials=True)



@app.route('/', methods=['POST'])
def splotlight():
    if request.method == 'POST':
        flow_id = flowManager.add_flow(request.json)
        flowManager.run_flow(flow_id)
        return 'success'
    return 'failed'

if __name__ == '__main__':
    app.debug = True
    app.run(host='127.0.0.1', port=5000)
