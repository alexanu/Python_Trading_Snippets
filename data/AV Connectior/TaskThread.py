# Source: http://code.activestate.com/recipes/65222-run-a-task-every-few-seconds/

import threading
import os
import sys
import inspect
import time

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)


class TaskThread(threading.Thread):
    """Thread that executes a task every N seconds"""

    def __init__(self):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self._timeout = threading.Event()
        self._finished = threading.Event()
        self._enabled = threading.Event()
        self._enabled.set()
        self._interval = 1  # default value
        self._singleRun = False
        self._start_delay = 0

    def setInterval(self, interval):
        """Set the number of seconds we sleep between executing our task"""
        self._interval = interval

    def shutdown(self):
        """Stop this thread"""
        self._timeout.set()
        self._enabled.set()
        self._finished.set()

    def enable(self, enabled):
        """Disable/enable this thread"""
        self._enabled.set() if enabled else self._enabled.clear()

    def is_enabled(self):
        return self._enabled.isSet()

    def cancel_timeout(self):
        """Cancel the timeout and run the task"""
        self._timeout.set()

    def force_single_run(self):
        self._singleRun = True
        self.enable(True)
        self.cancel_timeout()

    def run(self):
        time.sleep(self._start_delay)
        while 1:
            # reset timeout
            self._timeout.clear()
            # check shutdown flag
            if self._finished.isSet():
                return
            # sleep until enabled or return immediatly
            self._enabled.wait()
            # perform task
            self.task()
            # Check if it was a single run
            if self._singleRun:
                self.enable(False)
            self._singleRun = False
            # sleep for interval or until shutdown
            self._timeout.wait(self._interval)

    def task(self):
        """The task done by this thread - override in subclasses"""
        raise Exception("TaskThread: task function not overridden by children class!")

    def start_delayed(self, delay_s):
        self._start_delay = delay_s
        self.start()
