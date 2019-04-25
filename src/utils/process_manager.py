import multiprocessing
import threading

from flow.flow_status import FlowStatus


lock = threading.Lock()

def worker(process_flow_manager):
    while 1:

        proc = None
        proc_name = None
        lock.acquire()
        for name in process_flow_manager.running_process:
            proc = process_flow_manager.running_process[name]

            if not proc.is_alive():
                proc = process_flow_manager.running_process.pop(name)
                if proc.exitcode == FlowStatus.FAILED:
                    process_flow_manager.failed_process.add(proc_name)
                elif proc.exitcode == FlowStatus.SUCCESS:
                    process_flow_manager.success_process.add(proc_name)
                break
        lock.release()


class ProcessFlowManager:

    def __init__(self, max_process=5):
        self.running_process = {}
        self.pending_process = {}
        self.failed_process = set()
        self.success_process = set()
        self.max_process = 5
        self._worker_handler = threading.Thread(target=worker, args=(self,))
        self._worker_handler.start()

    def kill(self, process_name):
        self.running_process[process_name].kill()

    def terminate(self, process_name):
        self.running_process[process_name].terminate()

    def submit_flow(self, flow):
        proc = multiprocessing.Process(target=flow.run)
        proc.name = flow.flow_id
        lock.acquire()
        self.pending_process[proc.name] = proc
        lock.release()

    def run(self, process_name):
        if len(self.running_process) <= self.max_process:
            lock.acquire()
            proc = self.pending_process.pop(process_name)
            self.running_process[proc.name] = proc
            lock.release()
            proc.start()
