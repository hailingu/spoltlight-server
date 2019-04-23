import multiprocessing


class Task:
    '''scheduler running task'''

    def __init__(self, flow):
        self.flow = flow
        self.proc = multiprocessing.Process(target=flow.run)
        # self.proc.daemon = True

    def run(self):
        self.proc.start()

    def terminal(self):
        self.proc.terminate()

    def kill(self):
        self.proc.kill()