from operators.scikitlearn.scikitlearn_flow import ScikitlearnFlow
from operators.spark.spark_flow import SparkFlow


class FlowManager:
    '''A splotlight flow manager'''

    def __init__(self):
        self.flows = {
            'scikitlearn': {},
            'spark': {}
        }
    
    def add_flow(self, flow_json):
        backend = flow_json['flow']['backend']
        flow = None
        if backend == 'scikitlearn':
            flow = ScikitlearnFlow(flow_json)
            self.flows['scikitlearn'][flow.id] = flow

        if backend == 'spark':
            flow = SparkFlow(flow_json)
            self.flows['spark'][flow.id] = flow
        
        return flow.id

    def get_flow(self, flow_id):
        flow = None
        if flow_id in self.flows['scikitlearn']:
            flow = self.flows['scikitlearn'][flow_id]

        if flow_id in self.flows['spark']:
            flow = self.flows['spark'][flow_id]

        return flow

    def run_flow(self, flow_id):
        flow = self.get_flow(flow_id)
        flow.run()
    
flowManager = FlowManager()