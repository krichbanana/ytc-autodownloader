#!/usr/bin/env python3
import utils
import json

class Clock:
    """ Tracks current time.
        Periodically, the time updates. If a certain time has passed, an action is called.
    """
    def __init__(self):
        self.currtime = utils.get_utc_timestamp_now()
        self.targettime = None
        self.step = 1

    def set_target_time(self, target: float):
        self.targettime = target

    def set_next_target_time(self):
        self.targettime += self.step

    def set_step(self, step: int):
        self.step = step

    def tick(self, callback):
        now = utils.get_utc_timestamp_now()
        if self.targettime is not None and self.currtime < self.targettime and now >= self.targettime:
            callback()
        self.targettime += self.step
        if self.targettime < now:
            self.targettime = now
            self.set_next_target_time()
        
