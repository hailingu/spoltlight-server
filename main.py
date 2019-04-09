from flask import Flask
from flask_cors import CORS
from flask import request
import operators.operator
from operators.spark.spark_flow import SparkFlow
app = Flask(__name__)
CORS(app, support_credentials=True)



@app.route('/', methods=['POST'])
def hello_flask():
    if request.method == 'POST':
        # print(request.json)
        a = SparkFlow(request.json)
        a.generate()
        return 'success'
    return 'failed'

def create_azkaban_job(job_id):
    return 0


if __name__ == '__main__':
    app.debug = True
    app.run(host='127.0.0.1', port=5000)
