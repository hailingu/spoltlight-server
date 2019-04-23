from scheduler.scheduler import Scheduler
from scheduler.spotlight_scheduler import spotlightScheduler


class SchedulerFactory:

    @staticmethod
    def get_scheduler(flow):
        scheduler = spotlightScheduler
        flow.flow_scheduler_inst = scheduler
        return scheduler

    