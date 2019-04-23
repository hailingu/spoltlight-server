from multiprocessing import Pool

from scheduler.scheduler import Scheduler
from flow.flow_status import FlowStatus


class SpotlightScheduler(Scheduler):
    '''This is default spotlight server schedule'''

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        self.scheduler_pending_flows = {}
        self.scheduler_running_flows = {}
        self.scheduler_flow_proc_dic = {}
        self.scheduler_success_flows = {}
        self.scheduler_failded_flows = {}
        self.scheduler_thread_pools = Pool(processes=4)
    
    def submit(self, flow):
        self.scheduler_pending_flows[flow.flow_id] = flow

    def get_status(self, flow_id):
        if flow_id in self.scheduler_failded_flows:
            return FlowStatus.FAILED
        elif flow_id in self.scheduler_running_flows:
            return FlowStatus.RUNNING
        elif flow_id in self.scheduler_pending_flows:
            return FlowStatus.PENDING
        elif flow_id in self.scheduler_success_flows:
            return FlowStatus.SUCCESS
        else:
            return None

    def run(self, flow_id):
        flow = self.scheduler_pending_flows[flow_id]
        proc = self.scheduler_thread_pools.apply_async(flow.run())
        self.scheduler_flow_proc_dic[flow_id] = proc
        self.scheduler_running_flows[flow_id] = flow 

    def pause(self, flow_id):
        pass

    def stop(self, flow_id):
        pass
    
spotlightScheduler = SpotlightScheduler()