from scheduler.scheduler import Scheduler
from scheduler.spotlight_scheduler import spotlightScheduler


class SchedulerFactory:

    @staticmethod
    def get_scheduler(scheduler_name):
        return spotlightScheduler

    