import multiprocessing

from scheduler.scheduler import Scheduler
from flow.flow_status import FlowStatus
from utils.process_pool import ProcessPool
from utils.task import FlowTask


class SpotlightScheduler(Scheduler):
    '''This is default spotlight server schedule'''

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        self.scheduler_pending_flows = {}
        self.scheduler_running_flows = {}
        self.scheduler_success_flows = {}
        self.scheduler_failded_flows = {}
        self.scheduler_thread_pools = ProcessPool(processes=4)
    
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
        self.scheduler_running_flows[flow_id] = flow
        flow_task = FlowTask(flow)
        self.scheduler_thread_pools.apply_async(flow_task)

        # p = multiprocessing.Process(target=flow.run)
        # self.scheduler_flow_proc_dic[flow_id] = p
        # p.start()
        # print('aaaaa')
        # p.kill()
        # print('end')

        # self.scheduler_flow_proc_dic[flow_id] = self.scheduler_thread_pools.apply_async(flow.run)
        # multiprocessing.processes(target=flow.run())
        
    def pause(self, flow_id):
        pass

    def stop(self, flow_id):
        pass

    def shutdown(self):
        # self.scheduler_thread_pools.join()
        # self.scheduler_thread_pools.close()
        pass
    
    
spotlightScheduler = SpotlightScheduler()