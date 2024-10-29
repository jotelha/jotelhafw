# coding: utf-8
#
# multiprocessing.py
#
# Copyright (C) 2020 IMTEK Simulation
# Author: Johannes Hoermann, johannes.hoermann@imtek.uni-freiburg.de
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
"""Multiprocessing utils."""

from abc import abstractmethod
from typing import List


import multiprocessing  # run task as child process to avoid side effects
import traceback  # forward exception from child process to parent process

from fireworks.core.firework import FiretaskBase, FWAction


# in order to make sure any modifications to the environment within task
# won't pollute Fireworks process environmnet, run task in separate process
# https://stackoverflow.com/questions/19924104/python-multiprocessing-handling-child-errors-in-parent
class Process(multiprocessing.Process):
    """
    Class which returns child Exceptions to Parent.
    https://stackoverflow.com/a/33599967/4992248
    """

    def __init__(self, *args, **kwargs):
        multiprocessing.Process.__init__(self, *args, **kwargs)
        self._parent_conn, self._child_conn = multiprocessing.Pipe()
        self._exception = None

    def run(self):
        try:
            multiprocessing.Process.run(self)
            self._child_conn.send(None)
        except Exception as e:
            tb = traceback.format_exc()
            self._child_conn.send((e, tb))
            raise e  # You can still rise this exception if you need to

    @property
    def exception(self):
        if self._parent_conn.poll():
            self._exception = self._parent_conn.recv()
        return self._exception


class RunAsChildProcessTask(FiretaskBase):
    """Run task as child process to avoid side effects by environment pollution.

    Required params:
        None
    Optional params:
        None
    """
    _fw_name = 'RunAsChildProcessTask'
    required_params: List[str] = []
    optional_params: List[str] = []

    @abstractmethod
    def _run_task_as_child_process(
            self,
            fw_spec: dict,
            q: multiprocessing.Queue,
            e: multiprocessing.Event = None
        ) -> None:
        """Replaces run_task in derivatives.

        Child must use q: Queue to return FWAction, that means

            q.put(fw_action)

        replaces

            return fw_action

        If child does not queue fw_action, then parent process will dead lock.
        If set via method 'set_stop_event', then this event is passed on to
        the child to enable it to exit gacefully when desired. It must still
        return an FWAction object.
        """
        ...

    def run_task(self, fw_spec: dict) -> FWAction:
        """Spawn child process to assure my environment stays untouched."""
        q = multiprocessing.Queue()
        e = multiprocessing.Event()
        p = Process(
            target=self._run_task_as_child_process, args=(fw_spec, q, e))

        p.start()

        # wait for child to queue fw_action object and
        # check whether child raises exception
        while q.empty():
            # if child raises exception, then it has terminated
            # before queueing any fw_action object
            if p.exception:
                error, p_traceback = p.exception
                raise ChildProcessError(p_traceback)

            # let child process stop
            if hasattr(self, '_stop_event') and hasattr(self._stop_event, 'is_set'):
                if self._stop_event.is_set():
                    e.set()

        # this loop will deadlock for any child that never raises
        # an exception and does not queue anything

        # queue only used for one transfer of
        # return fw_action, should thus never deadlock.
        fw_action = q.get()
        # if we reach this line without the child
        # queueing anything, then process will deadlock.
        p.join()
        return fw_action

    def set_stop_event(self, e):
        self._stop_event = e
