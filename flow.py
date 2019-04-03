class Flow:
    '''A splotlight job flow'''

    def __init__(self, backend):
        self.id = None
        self.backend = backend
        self.flow_json = None
        self.scheduler = None

    def parse_flow(self):
        if self.backend == 'default':
            print('scikit flow')
        elif self.backend == 'spark':
            print('spark flow')
        elif self.backend == 'r':
            print('r flow')
        elif self.backend == 'mxnet':
            print('mxnet flow')
        elif self.backend == 'tensorflow':
            print('tensorflow')
        else:
            print('invalid flow')
        return None
    