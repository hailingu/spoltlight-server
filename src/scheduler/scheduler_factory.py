from scheduler.scheduler import Scheduler
from scheduler.spotlight_scheduler import spotlightScheduler
from scheduler.azkaban_scheduler import azkabanScheduler


class SchedulerFactory:
    '''spotlight server scheduler factory'''


    @staticmethod
    def submit_flow(flow):
        scheduler = None
        if flow.flow_scheduler == 'default':
            scheduler = spotlightScheduler
            flow.flow_scheduler_inst = scheduler
        elif flow.flow_scheduler == 'azkaban':
            scheduler = azkabanScheduler
            flow.flow_scheduler_inst = azkabanScheduler
        return scheduler

    @staticmethod
    def run_flow(flow_id):
        flow_status = None
        if flow_id in spotlightScheduler.scheduler_pending_flows:
            flow_status = spotlightScheduler.run(flow_id)
        elif flow_id in azkabanScheduler.scheduler_pending_flows:
            flow_status = azkabanScheduler.run(flow_id)

        return flow_status

    @staticmethod
    def shutdown():
        try:
            spotlightScheduler.shutdown()
            azkabanScheduler.shutdown()
        except Exception as e:
            print(e)


    