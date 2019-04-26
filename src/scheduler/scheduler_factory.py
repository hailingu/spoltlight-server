from scheduler.scheduler import Scheduler
from scheduler.spotlight_scheduler import spotlightScheduler
from scheduler.azkaban_scheduler import azkabanScheduler


class SchedulerFactory:
    '''spotlight server scheduler factory'''

    @staticmethod
    def get_scheduler(flow):
        if flow.flow_scheduler == 'default':
            scheduler = spotlightScheduler
            flow.flow_scheduler_inst = scheduler
        elif flow.flow_scheduler == 'azkaban':
            scheduler = azkabanScheduler
            flow.flow_scheduler_inst = azkabanScheduler
        return flow

    