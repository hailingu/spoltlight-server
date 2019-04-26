from flask import Flask, request
from flask_cors import CORS

from flow.flow_manager import FlowManager
from scheduler.scheduler_factory import SchedulerFactory

app = Flask(__name__)
CORS(app, support_credentials=True)

@app.route('/submit_flow', methods=['POST'])
def submit_flow():
    if request.method == 'POST':
        flow = FlowManager.spawn_flow(request.json)
        scheduler = SchedulerFactory.submit_flow(flow)
        scheduler.submit(flow)
        return 'success'
    return 'failed'

@app.route('/run_flow', method=['POST'])
def run_flow():
    if request.method == 'POST':
        flow_id = request.json['flow_id']



if __name__ == '__main__':
    app.debug = True
    app.run(host='127.0.0.1', port=5000, threaded=True)
    scheduler = SchedulerFactory.get_scheduler('')
    scheduler.shutdown()
    