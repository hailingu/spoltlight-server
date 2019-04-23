from flask import Flask, request
from flask_cors import CORS

from flow.flow_manager import FlowManager
from scheduler.scheduler_factory import SchedulerFactory

app = Flask(__name__)
CORS(app, support_credentials=True)


@app.route('/', methods=['POST'])
def splotlight():
    if request.method == 'POST':
        flow = FlowManager.spawn_flow(request.json)
        scheduler = SchedulerFactory.get_scheduler(flow)
        scheduler.submit(flow)
        scheduler.run(flow.flow_id)
        return 'success'
    return 'failed'

if __name__ == '__main__':
    app.debug = True
    app.run(host='127.0.0.1', port=5000, threaded=True)
    scheduler = SchedulerFactory.get_scheduler('')
    scheduler.shutdown()
    