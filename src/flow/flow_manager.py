from operators.scikitlearn.scikitlearn_flow import ScikitlearnFlow
from operators.spark.spark_flow import SparkFlow


class FlowManager:
    '''A splotlight flow manager'''

    @staticmethod    
    def spawn_flow(spawn_flow):
        backend = flow_json['flow']['backend']
        flow = None
        if backend == 'scikitlearn':
            flow = ScikitlearnFlow()
            flow.init(flow_json)

        if backend == 'spark':
            flow = SparkFlow()
            flow.init(flow_json)
        
        return flow