from scheduler.scheduler import Scheduler
from scheduler.spotlight_scheduler import spotlight_scheduler
from scheduler.azkaban_scheduler import azkaban_scheduler


class SchedulerFactory:
    '''spotlight server scheduler factory'''


    @staticmethod
    def submit_flow(flow):
        scheduler = None
        if flow.flow_scheduler == 'default':
            scheduler = spotlight_scheduler
            flow.flow_scheduler_inst = scheduler
        elif flow.flow_scheduler == 'azkaban':
            scheduler = azkaban_scheduler
            flow.flow_scheduler_inst = azkaban_scheduler
        return scheduler

    @staticmethod
    def run_flow(flow_id):
        flow_status = None
        if flow_id in spotlight_scheduler.scheduler_pending_flows:
            flow_status = spotlight_scheduler.run(flow_id)
        elif flow_id in azkaban_scheduler.scheduler_pending_flows:
            flow_status = azkaban_scheduler.run(flow_id)

        return flow_status

    @staticmethod
    def shutdown():
        try:
            spotlight_scheduler.shutdown()
            azkaban_scheduler.shutdown()
        except Exception as e:
            print(e)
