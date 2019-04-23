import multiprocessing


class Task(multiprocessing.Process):
    '''scheduler running task'''

    def __init__(self, task):
        self.task = task
        self.proc = multiprocessing.Process(target=task.run)
        # self.proc.daemon = True

    def run(self):
        self.proc.start()

    def terminal(self):
        self.proc.terminate()

    def kill(self):
        self.proc.kill()