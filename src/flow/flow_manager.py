from flow.scikitlearn_flow import ScikitlearnFlow
from flow.spark_flow import SparkFlow


class FlowManager:
    '''A splotlight flow manager'''

    @staticmethod    
    def spawn_flow(flow_json):
        backend = flow_json['flow']['backend']
        flow = None
        if backend == 'scikitlearn':
            flow = ScikitlearnFlow()
            flow.init(flow_json)

        if backend == 'spark':
            flow = SparkFlow()
            flow.init(flow_json)
        
        return flow
