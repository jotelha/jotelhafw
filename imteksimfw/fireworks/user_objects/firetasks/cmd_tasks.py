#!/usr/bin/env python
#
# cmd_tasks.py
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
"""Tasks that modify the environment by lookups in the worker file."""

import functools
import logging
import io
import multiprocessing
import os
import pickle
import subprocess
import sys
import traceback
import threading

from contextlib import ExitStack, redirect_stdout, redirect_stderr
from queue import Queue, Empty
from six.moves import builtins

# fireworks-internal
from fireworks.core.firework import FiretaskBase, FWAction
from fireworks.user_objects.firetasks.script_task import ScriptTask, PyTask

# in order to have a somewhat centralized encoding configuration
from fireworks.utilities.fw_serializers import ENCODING_PARAMS

from imteksimfw.utils.tracer import trace_func

__author__ = 'Johannes Hoermann'
__copyright__ = 'Copyright 2018, IMTEK'
__version__ = '0.1.1'
__maintainer__ = 'Johannes Hoermann'
__email__ = 'johannes.hoermann@imtek.uni-freiburg.de'
__date__ = 'Mar 18, 2020'


# TODO: remove too verbose logging for pipes and streams

# TODO: own logging format on this level

# nice post on reading and writing streams:
# https://stackoverflow.com/questions/33395004/python-streamio-reading-and-writing-from-the-same-stream

# source: # https://stackoverflow.com/questions/4984428/python-subprocess-get-childrens-output-to-file-and-terminal
def tee(infile, *files):
    """Print `infile` to `files` in a separate thread."""
    def fanout(infile, *files):
        with infile:
            for line in iter(infile.readline, ''):
                for f in files:
                    f.write(line)

    t = threading.Thread(target=fanout, args=(infile,)+files)
    t.daemon = True
    t.start()
    return t


# NonBlockingStreamReader might be obsolete as long as all streams to be read
# are flushed and closed properly, eventually sending zero
# https://gist.github.com/EyalAr/7915597
class NonBlockingStreamReader:
    def __init__(self, stream):
        '''
        stream: the stream to read from.
                Usually a process' stdout or stderr.
        '''
        self.logger = logging.getLogger(__name__)
        self._s = stream
        self._q = Queue()

        def _populate_queue(stream, queue):
            '''
            Collect lines from 'stream' and put them in 'queue'.
            '''
            for line in iter(stream.readline, b''):
                if line is not None and len(line) > 0:
                    self.logger.debug("Queueing line '{}'.".format(line))
                queue.put(line)

        self._t = threading.Thread(target=_populate_queue,
            args=(self._s, self._q), name="NonBlockingStreamReader")
        self._t.daemon = True
        self._t.start()  # start collecting lines from the stream

    def readline(self, timeout=None):
        try:
            line = self._q.get(block=timeout is not None,
                timeout=timeout)
            if line is not None and len(line) > 0:
                self.logger.debug("Popped line '{}'.".format(line))
            return line
        except Empty:
            self.logger.debug("Nothing popped.")
            return None


# https://www.oreilly.com/library/view/python-cookbook/0596001673/ch06s03.html
class Tee(threading.Thread):
    """Tee of instream to an arbitrary number of outstreams."""
    def __init__(self, instream, *outstreams, **kwargs):
        """ constructor, setting initial variables """
        self.logger = logging.getLogger(__name__)
        self._stopevent = threading.Event()
        # self._sleepperiod = 1.0e-3  # s
        self.instream = instream
        self.outstreams = outstreams
        threading.Thread.__init__(self, **kwargs)

    def run(self):
        """ main control loop """
        # following line allows for this thread not to block when waiting
        # for data on a stream, but might also cause ending before the stream
        # is finished
        # instream = NonBlockingStreamReader(self.instream)
        instream = self.instream
        # finish reading in any case if there is still stuff coming through
        line = None
        # line is '' if stream ended
        while (not self._stopevent.isSet()) or (line != ''):
            if self._stopevent.isSet():
                # thread is about to stop, try to get remaining contents in stream
                # try to flush instream (if it's both readable AND writable)
                self.logger.debug(
                    "{} received stop signal, reading remaining stream data."
                        .format(self.name))
                try:
                    instream.flush()
                except Exception:
                    pass

            line = instream.readline()
            if line is not None:
                # if hasattr(self, 'name') and line is not None and len(line) > 0:
                #    self.logger.debug("{} captured '{}'".format(self.name, line))
                for f in self.outstreams:
                    f.write(line)
            # self._stopevent.wait(self._sleepperiod)

        for f in self.outstreams:
            f.flush()
        if hasattr(self, 'name'):
            self.logger.debug("{} received stop signal and finished.".format(self.name))

    def join(self, timeout=None):
        """ Stop the thread. """
        self._stopevent.set()
        if hasattr(self, 'name'):
            self.logger.debug("Ending thread '{}'".format(self.name))
        threading.Thread.join(self, timeout)


class TeeContext():
    """Tee context manager."""
    def __init__(self, instream, *outstreams, **kwargs):
        self.logger = logging.getLogger(__name__)
        self.instream = instream
        self.outstreams = outstreams
        self.tee = None
        self.kwargs = kwargs

    def __enter__(self):
        if 'name' in self.kwargs:
            self.logger.debug("Entering '{}' context.".format(self.kwargs['name']))
        self.tee = Tee(self.instream, *self.outstreams, **self.kwargs)
        self.tee.start()

    def __exit__(self, et, ev, tb):
        if 'name' in self.kwargs:
            self.logger.debug("Leaving '{}' context.".format(self.kwargs['name']))
        # self.tee.join(0.0)
        self.tee.join()


class PipeRWPair(io.BufferedRWPair):
    """Wraps io.BufferedRWPair around binary os.pipe."""
    def __init__(self, *args, name=None, **kwargs):
        self.logger = logging.getLogger(__name__)
        self.instance_name = name

        self._pipeout, self._pipein = os.pipe()
        if self.instance_name is not None:
            self.logger.debug(
                "{:s} opened pipe with infile {:d} and outfile {:d}."
                    .format(self.instance_name, self._pipein, self._pipeout))

        self.pipe_reader = io.FileIO(self._pipeout, mode='rb')
        self.pipe_writer = io.FileIO(self._pipein, mode='wb')
        super().__init__(self.pipe_reader, self.pipe_writer, *args, **kwargs)


class TextIOPipe(io.TextIOWrapper):
    """Wraps io.TextIOWrapper around PipeRWPair."""
    def __init__(self, *args, name=None, **kwargs):
        self.logger = logging.getLogger(__name__)
        self.instance_name = name

        self.pipe = PipeRWPair(name=name)
        super().__init__(self.pipe, *args, **kwargs)


class TextIOPipeContext(TextIOPipe):
    """Flushed underlying pipe writer when leaving context."""
    def __exit__(self, et, ev, tb):
        if self.instance_name is not None:
            self.logger.debug("Flushing {:s}.".format(self.instance_name))
        self.flush()


class LoggingContext():
    """Modifies logging within context."""
    def __init__(self, logger=None, handler=None, level=None, close=False):
        self.logger = logger
        self.level = level
        self.handler = handler
        self.close = close

    def __enter__(self):
        """Prepare log output streams."""
        if self.logger is None:
            # temporarily modify root logger
            self.logger = logging.getLogger('')
        if self.level is not None:
            self.old_level = self.logger.level
            self.logger.setLevel(self.level)
        if self.handler:
            self.logger.addHandler(self.handler)

    def __exit__(self, et, ev, tb):
        if self.level is not None:
            self.logger.setLevel(self.old_level)
        if self.handler:
            self.logger.removeHandler(self.handler)
        if self.handler and self.close:
            self.handler.close()
        # implicit return of None => don't swallow exceptions

# TODO: there seems to be an issue with unclean temporary environment
# at CmdTask only resolvable after rlaunch restart:
# NameError: name 'false' is not defined
class TemporaryOSEnviron:
    """Preserve original os.environ context manager."""

    def __enter__(self):
        """Store backup of current os.environ."""
        self._original_environ = os.environ.copy()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Restore backed up os.environ."""
        os.environ = self._original_environ


# TODO: context for temporarily modified sys.path & sites, probably not perfect
class TemporarySysPath:
    """Preserve original os.environ context manager."""

    def __enter__(self):
        """Store backup of current sys.path."""
        self._original_sys_path = sys.path.copy()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Restore backed up sys.path."""
        sys.path = self._original_sys_path


def trace_method(func):
    """Decorator to trace own methods."""
    @functools.wraps(func)
    def wrapper_trace_method(self, *args, **kwargs):
        if hasattr(self, '_call_log_stream'):
            call_log_stream = self._call_log_stream
        else:
            call_log_stream = sys.stderr

        if hasattr(self, '_vars_log_stream'):
            vars_log_stream = self._vars_log_stream
        else:
            vars_log_stream = sys.stderr

        return trace_func(
            module=__name__,
            call_printer_stream=call_log_stream,
            vars_snooper_stream=vars_log_stream,
         )(func)(self, *args, **kwargs)
    return wrapper_trace_method


def get_nested_dict_value(d, key):
    """Uses '.'-splittable string as key to access nested dict."""
    if key in d:
        val = d[key]
    else:
        key = key.replace("->", ".")  # make sure no -> left
        split_key = key.split('.', 1)
        if len(split_key) == 2:
            key_prefix, key_suffix = split_key[0], split_key[1]
        else:  # not enough values to unpack
            raise KeyError("'{:s}' not in {}".format(key, d))

        val = get_nested_dict_value(d[key_prefix], key_suffix)

    return val

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


class EnvTask(FiretaskBase):
    """Abstract base class for tasks that modify the environmen.

    All derivatives will have the option to
      * specify an environment 'env' to be looked up within the worker file,
      * redirect stdout, stderr and log streams to files 'stdout_file',
        'stderr_file', and 'stdlog_file'.
      * store stdout and stderr and log messages in database if 'stored_stdout',
        'store_stderr', 'store_stdlog' are set,
      * trace execution and stream calls and variable changes to
        'call_log_file' and 'vars_log_file' if the package 'hunter' is
        available.
      * write simple (non-exhaustive) pseudo command history to 'py_hist_file'.

    Examples:

        For testing redirection of output and logging information to different
        locations, use a simple bash snippet `trial.sh`

            #!/bin/bash
            echo "This goes to stdout!"
            echo "This goes to stderr!" 1>&2

        and evoke via CmdTask with

        >>> from imteksimfw.fireworks.user_objects.firetasks.cmd_tasks import CmdTask
        >>>
        >>> import logging
        >>>
        >>> logformat = (
        >>>             "[%(asctime)s-%(funcName)s()-%(filename)s:%(lineno)s]"
        >>>             " %(levelname)s: %(message)s")
        >>> logging.basicConfig(
        >>>     level=logging.DEBUG, format=logformat)
        >>>
        >>> task = CmdTask(
        >>>     cmd='bash',
        >>>     opt='trial.sh',
        >>>     stderr_file  = 'std.err',
        >>>     stdout_file  = 'std.out',
        >>>     stdlog_file  = 'std.log',
        >>>     py_hist_file = 'simple_hist.py',
        >>>     call_log_file= 'calls.log',
        >>>     vars_log_file= 'vars.log',
        >>>     store_stdout = True,
        >>>     store_stderr = True,
        >>>     store_stdlog = True,
        >>>     use_shell    = False,
        >>>     fizzle_bad_rc= True)
        >>>
        >>> fwaction = task.run_task({})
        >>> print(fwaction)

        and investigate the contents of 'std.err', 'std.out', 'std.log',
        'simple_hist.py', 'calls.log' and 'vars.log' as well as what you see
        as output on the terminal.
    """

    # required_params = []
    other_params = [
        # base class EnvTask params
        'env',
        'loglevel',
        'py_hist_file',
        'call_log_file',
        'vars_log_file',
        'stdout_file',
        'stderr_file',
        'stdlog_file',
        'store_stdout',
        'store_stderr',
        'store_stdlog',
        'propagate',  # propagate update_spec and mod_spec down all decendants
    ]

    def _py_hist_append(self, line):
        # self._py_hist_stream.write(line + '\n')
        if self._py_hist_logger is not None:
            self._py_hist_logger.info(line)

    def run_task(self, fw_spec):
        """Run a sub-process. Modify environment if desired."""
        if self.get('use_global_spec'):
            self._load_params(fw_spec)
        else:
            self._load_params(self)

        # spawn child process to assure my environment stays untouched
        q = multiprocessing.Queue()
        p = Process(target=self._run_as_child_process, args=(q, fw_spec,))
        p.start()

        # wait for child to queue fw_action object and
        # check whether child raises exception
        while q.empty():
            # if child raises exception, then it has terminated
            # before queueing any fw_action object
            if p.exception:
                error, p_traceback = p.exception
                raise ChildProcessError(p_traceback)
        # this loop will deadlock for any child that never raises
        # an exception and does not queue anything

        # child has finished without exception
        # child must always return fw_action
        # queue only used for one transfer of
        # return fw_action, should thus never deadlock.
        fw_action = q.get()
        # if we reach this line without the child
        # queueing anything, then process will deadlock.
        p.join()

        return fw_action

    def _run_as_child_process(self, q, fw_spec):
        threads = []

        with ExitStack() as stack:
            # contexts to be entered before and left in reverse order after
            # task execution

            # logging to dedicated log stream if desired
            if self.store_stdlog:
                # stdlog_stream = io.TextIOWrapper(io.BytesIO(), **ENCODING_PARAMS)
                stdlog_stream = io.StringIO()
                logh = logging.StreamHandler(stdlog_stream)
                # optional 1st context: logging to in-memory buffer context
                stack.enter_context(
                    LoggingContext(handler=logh, level=self.loglevel, close=False))

            # logging to dedicated log file if desired
            if self.stdlog_file:
                logfh = logging.FileHandler(self.stdlog_file, mode='a', **ENCODING_PARAMS)
                # optional 2nd context: logging to file context
                stack.enter_context(
                    LoggingContext(handler=logfh, level=self.loglevel, close=True))


            # redirect stdout and stderr if desired (store them in db later)
            # create non-default streams to insert into db if desired
            out_streams = []
            if self.stdout_file:
                outf = open(self.stdout_file, 'a', **ENCODING_PARAMS)
                out_streams.append(outf)
            if self.store_stdout:
                stdout_stream = io.StringIO()  # io.TextIOWrapper(io.BytesIO(), **ENCODING_PARAMS)
                out_streams.append(stdout_stream)
            out_streams.append(sys.stdout)
            # stdout_pipe = TextIOPipeContext(name='StdOutPipe', **ENCODING_PARAMS)
            stdout_pipeout, stdout_pipein = os.pipe()

            # 3rd context: stdout tee
            stdout_reader = stack.enter_context(
                io.TextIOWrapper(io.FileIO(stdout_pipeout, mode='rb'), **ENCODING_PARAMS))

            stack.enter_context(
                TeeContext(stdout_reader, *out_streams, name='StdOutTee'))

            # 4th context: stdout pipe
            # stack.enter_context(stdout_pipe)

            err_streams = []
            if self.stderr_file:
                errf = open(self.stderr_file, 'a', **ENCODING_PARAMS)
                err_streams.append(errf)
            if self.store_stderr:
                stderr_stream = io.StringIO()
                err_streams.append(stderr_stream)
            err_streams.append(sys.stderr)
            # stderr_pipe = TextIOPipeContext(name='StdErrPipe', **ENCODING_PARAMS)
            stderr_pipeout, stderr_pipein = os.pipe()

            # 5th context: stderr tee
            stderr_reader = stack.enter_context(
                io.TextIOWrapper(io.FileIO(stderr_pipeout, mode='rb'), **ENCODING_PARAMS))

            stack.enter_context(
                TeeContext(stderr_reader, *err_streams, name='StdErrTee'))

            # 6th context stderr pipe
            # stack.enter_context(stderr_pipe)

            # always "tunnel" stdout and stderr through our buffers for
            # arbitrary tee'ing and capturing
            # 7th context: redirect sys.stdout into pipe
            # self.pipe_writer = io.FileIO(self._pipein, mode='wb')
            stdout_writer = stack.enter_context(
                io.TextIOWrapper(io.FileIO(stdout_pipein, mode='wb'), **ENCODING_PARAMS))

            stack.enter_context(redirect_stdout(stdout_writer))
            # stack.enter_context(redirect_stdout(stdout_pipe))
            # 8th context: redirect sys.stderr into pipe
            # stack.enter_context(redirect_stderr(stderr_pipe))
            stderr_writer = stack.enter_context(
                io.TextIOWrapper(io.FileIO(stderr_pipein, mode='wb'), **ENCODING_PARAMS))

            stack.enter_context(redirect_stderr(stderr_writer))

            # write pseudo command history to file if desired
            if self.py_hist_file:
                py_hist_handler = logging.FileHandler(self.py_hist_file, mode='a', **ENCODING_PARAMS)
                py_hist_formatter = logging.Formatter('%(message)s')
                py_hist_handler.setFormatter(py_hist_formatter)
            else:
                py_hist_handler = logging.NullHandler()
            self._py_hist_logger = logging.getLogger(__name__ + '.py_hist_logger')
            self._py_hist_logger.propagate = False
            self._py_hist_logger.setLevel(logging.DEBUG)
            for h in self._py_hist_logger.handlers:
                self._py_hist_logger.removeHandler(h)
            self._py_hist_logger.addHandler(py_hist_handler)

            # write tracer call logs to file if desired
            if self.call_log_file:
                self._call_log_stream = open(self.call_log_file, mode='a', **ENCODING_PARAMS)
                # optional 9th context: trace calls to file
                stack.enter_context(self._call_log_stream)
            else:
                self._call_log_stream = os.devnull

            # write tracer variable change logs to file if desired
            if self.vars_log_file:
                self._vars_log_stream = open(self.vars_log_file, mode='a', **ENCODING_PARAMS)
                # optional 10th context: trace variable changes to file
                stack.enter_context(self._vars_log_stream)
            else:
                self._vars_log_stream = os.devnull

            # log, stdout, stderr streams are logged to
            # files and database, depending on flags.

            # 11th context: allow temporary os.environ modifications within block
            stack.enter_context(TemporaryOSEnviron())
            # 12th context: allow temporary sys.path modifications within block
            stack.enter_context(TemporarySysPath())

            # for a variable amount of contexts:
            # for mgr in context_managers:
            #    stack.enter_context(mgr)
            # TODO: replace own logger attribute with getLogger in methods
            self.logger = logging.getLogger(__name__)
            ret = self._run_task_internal(fw_spec)

        for thread in threads:
            thread.join()  # wait for stream tee threads

        if isinstance(ret, FWAction):
            fw_action = ret
        else:
            fw_action = FWAction()

        output = {}
        if self.store_stdout:
            stdout_stream.flush()
            # stdout_stream.seek(0)
            # output['stdout'] = stdout_stream.read()
            output['stdout'] = stdout_stream.getvalue()

        if self.store_stderr:
            stderr_stream.flush()
            # stderr_stream.seek(0)
            # output['stderr'] = stderr_stream.read()
            output['stderr'] = stderr_stream.getvalue()

        if self.store_stdlog:
            stdlog_stream.flush()
            # stdlog_stream.seek(0)
            # output['stdlog'] = stdlog_stream.read()
            output['stdlog'] = stdlog_stream.getvalue()

        fw_action.stored_data.update(output)

        # 'propagate' only development feature for now
        if hasattr(fw_action, 'propagate') and self.propagate is not None:
            fw_action.propagate = self.propagate

        # return fw_action
        q.put(fw_action)

    # TODO: get rid of those _load_params functions (relic from ScriptTask)
    def _load_params(self, d):
        self.env = self.get('env')
        self.loglevel = self.get('loglevel', logging.DEBUG)
        self.py_hist_file = self.get('py_hist_file', 'simple_hist.py')
        self.call_log_file = self.get('call_log_file', 'calls.log')
        self.vars_log_file = self.get('vars_log_file', 'vars.log')

        self.stdout_file = d.get('stdout_file', 'std.out')
        self.stderr_file = d.get('stderr_file', 'std.err')
        self.stdlog_file = d.get('stdlog_file', 'std.log')
        self.store_stdout = d.get('store_stdout')
        self.store_stderr = d.get('store_stderr')
        self.store_stdlog = d.get('store_stdlog')

        self.propagate = d.get('propagate')


class CmdTask(EnvTask, ScriptTask):
    """Enhanced script task, runs (possibly environment dependent)  command.

    Required params:
        - cmd (str): command to look up within '_fw_env (see below for details)

    Optional params:
        - opt ([obj]): list of strings (or int, float, ...) to pass as
            options / arguments tp 'cmd'. List may contain dicts of
            pattern {'key': 'some->nested->fw_spec->key'} or
            {'eval': 'python-expression'} in order to dynamically modify opt.
        - env (str): allows to specify an environment possibly defined in the
            worker file. If so, additional environment-related iintialization
            and expansion of command aliases are carried out (see below).
        - defuse_bad_rc - (default:False) - if set True, a non-zero returncode
            from the Script (indicating error) will cause FireWorks to defuse
            the child FireWorks rather than continuing.
        - fizzle_bad_rc - (default:False) - if set True, a non-zero returncode
            from the Script (indicating error) will cause the Firework to raise
            an error and FIZZLE.
        - use_shell - (default:True) - whether to execute the command through
            the current shell (e.g., BASH or CSH). If true, you will be able
            to use environment variables and shell operators; but, this method
            can be less secure.
        - shell_exe - (default:None) - shell executable, e.g. /bin/bash.
            Generally, you do not need to set this unless you want to run
            through a non-default shell.
        - stdin_file - (default:None) - feed this filepath as standard input
            to the script
        - stdin_key - (default:None) - feed this string or list of strings
            as standard input to the script
        - store_stdout (default:False) - store the entire standard output in
            the Firework Launch object's stored_data
        - stdout_file - (default:None) - store the entire standard output in
            this filepath. If None, the standard out will be streamed to
            sys.stdout
        - store_stderr - (default:False) - store the entire standard error in
            the Firework Launch object's stored_data
        - stderr_file - (default:None) - store the entire standard error in
            this filepath. If None, the standard error will be streamed to
            sys.stderr.
        - propagate (bool, default:None): if True, then set the
            FWAction 'propagate' flag and propagate all 'update_spec' and
            'mod_spec' not only to direct children, but to all descendants
            down to the wokflow's leaves.

    Makes use of '_fw_env' worker specific definitions and allow for certain
    abstraction when running commands, i.e.

        - _fw_name: CmdTask
          cmd: lmp
          opt:
          - -in lmp_production.input
          - -v surfactant_name SDS
          - -v has_indenter 1
          - -v constant_indenter_velocity -0.0001
          - -v productionSteps 375000
          [...]
          - -v store_forces 1
          stderr_file:   std.err
          stdout_file:   std.out
          fizzle_bad_rc: true
          use_shell:     true

    will actually look for a definition 'lmp' within a worker's '_fw_env' and
    if available execute in place of the  specififed command, simply appending
    the list of options given in 'opt' separated by sapces. This allows
    machine-specific cod to be placed within the worker files, i.e. for a
    machine called NEMO within nemo_queue_worker.yaml

        name: nemo_queue_worker
        category: [ 'nemo_queue' ]
        query: '{}'
        env:
          lmp: module purge;
               module use /path/to/modulefiles;
               module use ${HOME}/modulefiles;
               module load lammps/16Mar18-gnu-7.3-openmpi-3.1-colvars-09Feb19;
               mpirun ${MPIRUN_OPTIONS} lmp

    and for a machine called JUWELS within

        name:     juwels_queue_worker
        category: [ 'juwels_queue' ]
        query:    '{}'
        env:
          lmp:  module purge;
                jutil env activate -p chfr13;
                module use ${PROJECT}/hoermann/modules/modulefiles;
                module load Intel/2019.0.117-GCC-7.3.0 IntelMPI/2018.4.274;
                module load jlh/lammps/16Mar18-intel-2019a
                srun lmp

    A third machine's worker file might look like this:

        name: bwcloud_std_fworker
        category: [ 'bwcloud_std', 'bwcloud_noqueue' ]
        query: '{}'
        env:
          lmp:  module purge;
                module load LAMMPS;
                mpirun -n 4 lmp
          vmd:  module purge;
                module load VMD;
                vmd
          pizza.py: module load MDTools/jlh-25Jan19-python-2.7;
                    pizza.py

    This allows for machine-independent workflow design.

    More sophisticated environment modifications are possible when
    explicitly specifying the 'env' option. Worker files can contain

        env:
            python:
                init:
                - 'import sys, os'
                - 'sys.path.insert(0, os.path.join(os.environ["MODULESHOME"], "init"))'
                - 'from env_modules_python import module'
                - 'module("use","/path/to/modulefiles")'
                cmd:
                    gmx:
                        init: 'module("load","GROMACS/2019.3")'
                        substitute: gmx_mpi
                        prefix: ['mpirun', { 'eval': 'os.environ["MPIRUN_OPTIONS"]' } ]

    corresponds to a bash snippet

        module use /path/to/modulefiles
        module load GROMACS/2019.3
        mpirun ${MPIRUN_OPTIONS} gmx_mpi ...

    extended by arguments within the task's 'opt' parameter.

    """
    _fw_name = 'CmdTask'
    required_params = ['cmd']
    optional_params = [
        # base class EnvTask params
        *EnvTask.other_params,
        # CmdTask params
        'opt',
        'stdin_file',
        'stdin_key',
        'use_shell',
        'shell_exe',
        'defuse_bad_rc',
        'fizzle_bad_rc'
    ]

    @property
    def args(self):
        """List of arguments (including command) as list of str only."""
        return [a if isinstance(a, str) else str(a) for a in self._args]

    @trace_method
    def _run_task_internal(self, fw_spec):
        """Run task."""
        stdout = subprocess.PIPE # if self.store_stdout or self.stdout_file else None
        stderr = subprocess.PIPE # if self.store_stderr or self.stderr_file else None

        # get the standard in and run task internally
        if self.stdin_file:
            stdin = open(self.stdin_file, 'r', **ENCODING_PARAMS)
        elif self.stdin_key:
            stdin_str = fw_spec[self.stdin_key]
            try:
                stdin_list = [stdin_str] if isinstance(stdin_str, str) \
                    else [str(line) for line in stdin_str]
            except:
                raise ValueError(("stdin_key '{}' must point to either string"
                                  " or list of strings, not '{}'.")
                                  .format(self.stdin_key, stdin_str))

            stdin = subprocess.PIPE
        else:
            stdin = None

        ### START WRITING PY_HIST
        self._py_hist_append('import os')
        self._py_hist_append('import subprocess')

        self._args = []
        # in case of a specified worker environment

        # in case of a specified worker environment
        if self.env and "_fw_env" in fw_spec \
                and self.env in fw_spec["_fw_env"]:
            self.logger.info("Found {:s}-specific block '{}' within worker file."
                .format(self.env, fw_spec["_fw_env"]))

            # """Parse global init block."""
            # _fw_env : env : init may provide a list of python commans
            # to run, i.e. for module env initialization
            if "init" in fw_spec["_fw_env"][self.env]:
                init = fw_spec["_fw_env"][self.env]["init"]
                if isinstance(init, str):
                    init = [init]
                assert isinstance(init, list)
                for cmd in init:
                    self.logger.info("Execute '{:s}'.".format(cmd))
                    self._py_hist_append(cmd)
                    exec(cmd)

            # """Parse global envronment block."""
            # per default, process inherits current environment
            # self.modenv = os.environ.copy()
            # modify environment before call if desired
            if "env" in fw_spec["_fw_env"][self.env]:
                env_dict = fw_spec["_fw_env"][self.env]["env"]
                if not isinstance(env_dict, dict):
                    raise ValueError(
                        "type({}) = {} of 'env' not accepted, must be dict!"
                            .format(env_dict, type(env_dict)))

                # so far, only simple overrides, no pre- or appending
                for i, (key, value) in enumerate(env_dict.items()):
                    self.logger.info("Set env var '{:s}' = '{:s}'.".format(
                        key, value))
                    self._py_hist_append('os.environ["{:s}"] = "{:s}"'.format(
                        str(key), str(value)))
                    os.environ[str(key)] = str(value)

        if self.env and "_fw_env" in fw_spec \
                and self.env in fw_spec["_fw_env"]:
            # check whether there is any machine-specific "expansion" for
            # the command head
            """Parse command-specific environment block."""
            if "cmd" in fw_spec["_fw_env"][self.env] \
                    and self.cmd in fw_spec["_fw_env"][self.env]["cmd"]:
                self.logger.info("Found {:s}-specific block '{}' within worker file."
                    .format(self.cmd, fw_spec["_fw_env"][self.env]["cmd"][self.cmd]))
                # same as above, evaluate command-specific initialization code
                """Parse per-command init block."""
                cmd_block = fw_spec["_fw_env"][self.env]["cmd"][self.cmd]
                if "init" in cmd_block:
                    init = cmd_block["init"]
                    if isinstance(init, str):
                        init = [init]
                    assert isinstance(init, list), "'init' must be str or list"
                    for cmd in init:
                        self.logger.info("Execute '{:s}'.".format(cmd))
                        self._py_hist_append(cmd)
                        exec(cmd)

                """Parse per-command substitute block."""
                cmd_block = fw_spec["_fw_env"][self.env]["cmd"][self.cmd]
                if "substitute" in cmd_block:
                    substitute = cmd_block["substitute"]
                    assert isinstance(substitute, str), "substitute must be str"
                    self.logger.info("Substitute '{:s}' with '{:s}'.".format(
                        self.cmd, substitute))
                    self._args.append(substitute)
                else:  # otherwise just use command as specified
                    self.logger.info("No substitute for command '{:s}'.".format(
                        self.cmd))
                    self._args.append(self.cmd)

                """Parse per-command prefix block."""
                cmd_block = fw_spec["_fw_env"][self.env]["cmd"][self.cmd]
                # prepend machine-specific prefixes to command
                if "prefix" in cmd_block:
                    prefix_list = cmd_block["prefix"]
                    if not isinstance(prefix_list, list):
                        prefix_list = [prefix_list]

                    processed_prefix_list = []
                    for i, prefix in enumerate(prefix_list):
                        processed_prefix = []
                        # a prefix dict allow for something like this:
                        #    cmd:
                        #      lmp:
                        #        init:   'module("load","lammps")'
                        #        prefix: [ 'mpirun', { 'eval': 'os.environ["MPIRUN_OPTIONS"]' } ]
                        if isinstance(prefix, dict):
                            # special treatment desired for this prefix
                            if "eval" in prefix:
                                self.logger.info("Evaluate prefix '{:s}'.".format(
                                    ' '.join(prefix["eval"])))
                                # evaluate prefix in current context
                                processed_prefix = eval(prefix["eval"])
                                try:
                                    processed_prefix = processed_prefix.decode("utf-8")
                                except AttributeError:
                                    pass
                                if isinstance(processed_prefix, str):
                                    processed_prefix = processed_prefix.split()
                                else:
                                    raise ValueError(
                                        "Output {} of prefix #{} evaluation not accepted!".format(
                                            processed_prefix, i))
                            else:
                                raise ValueError(
                                    "Formatting {} of prefix #{} not accepted!".format(
                                        prefix, i))
                        elif isinstance(prefix, str):
                            # prefix is string, not much to do, split & prepend
                            processed_prefix = prefix.split()
                        else:
                            raise ValueError(
                                "type({}) = {} of prefix #{} not accepted!".format(
                                    prefix, type(prefix), i))

                        if not isinstance(processed_prefix, list):
                            processed_prefix = [processed_prefix]

                        self.logger.info("Prepend prefix '{:s}'.".format(
                            ' '.join(processed_prefix)))
                        processed_prefix_list.extend(processed_prefix)

                    self._args = processed_prefix_list + self._args  # concat two lists

                """Parse command-specific environment block."""
                # per default, process inherits current environment

                cmd_block = fw_spec["_fw_env"][self.env]["cmd"][self.cmd]
                # modify environment before call if desired
                if "env" in cmd_block:
                    env_dict = cmd_block["env"]
                    if not isinstance(env_dict, dict):
                        raise ValueError(
                            "type({}) = {} of 'env' not accepted, must be dict!"
                            .format(env_dict, type(env_dict)))

                    # so far, only simple overrides, no pre- or appending
                    for i, (key, value) in enumerate(env_dict.items()):
                        self.logger.info("Set env var '{:s}' = '{:s}'.".format(
                            key, value))
                        self._py_hist_append('os.environ["{:s}"] = "{:s}"'.format(
                            str(key), str(value)))
                        os.environ[str(key)] = str(value)
            else:  # no command-specific expansion in environment, use as is
                self._args.append(self.cmd)
        elif "_fw_env" in fw_spec and self.cmd in fw_spec["_fw_env"]:
            # check whether there is any desired command and whether there
            # exists a machine-specific "alias"
            self.logger.info("Found root-level {:s}-specific block '{}' within worker file."
                .format(self.cmd, fw_spec["_fw_env"][self.cmd]))
            self._args = [fw_spec["_fw_env"][self.cmd]]
        else:
            self._args = [self.cmd]

        if self.opt:  # extend by command line options if available
            # evaluate raplacement rules, i.e. fw_spec key or expression evaluation
            for arg in self.opt:
                if isinstance(arg, dict):
                    # only one rule per opt item allowed
                    # insert value from (nested) key - value pair in fw_spec
                    if 'key' in arg:
                        arg = get_nested_dict_value(fw_spec, arg['key'])
                    # evaluate expression and insert result
                    elif 'eval' in arg:
                        arg = eval(arg['eval'])
                # user should only use strings as args
                # to be sure, always append string representation
                self._args.append(str(arg))

        self.logger.info("Built args '{}'.".format(self._args))
        self.logger.info("Built command '{:s}'.".format(
            ' '.join(self.args)))
        self.logger.debug("Process environment '{}'.".format(os.environ))

        kwargs = {}
        if self.shell_exe:
            kwargs.update({'executable': self.shell_exe})
        if self.use_shell:
            kwargs.update({'shell': self.use_shell})

        # Write .py script to reconstruct environment, very rudimentary.
        # The idea of this "history" file (just text written to a file,
        # no actual history) is to be able to reproduce runs quickly
        # outside of the FireWorks frameworks for debugging purposes.
        encoding_params_str_list = [
            '{}="{}"'.format(k, v) if isinstance(v, str)
            else '{}={}'.format(k, v) for k, v in ENCODING_PARAMS.items()]
        encoding_params_str = ', '.join(encoding_params_str_list)

        kwargs_str_list = [
            '{}="{}"'.format(k, v) if isinstance(v, str)
            else '{}={}'.format(k, v) for k, v in kwargs.items()]
        kwargs_str = ', '.join(kwargs_str_list)

        self._py_hist_append('# os.environ = {}'.format(dict(os.environ)))

        stdoutstr = 'subprocess.PIPE' # if self.store_stdout or self.stdout_file else 'None'
        stderrstr = 'subprocess.PIPE' # if self.store_stderr or self.stderr_file else 'None'
        if self.stdin_key:
            stdinstr = 'subprocess.PIPE'
        elif self.stdin_file:
            stdinstr = 'open("{:s}", "r", {:s})'.format(
                self.stdin_file, encoding_params_str)
        else:
            stdinstr = 'None'

        self._py_hist_append('')
        self._py_hist_append('p = subprocess.Popen(')
        self._py_hist_append('    {},'.format(self.args))
        self._py_hist_append('    stdin={},'.format(stdinstr))
        self._py_hist_append('    stdout={},'.format(stdoutstr))
        self._py_hist_append('    stderr={},'.format(stderrstr))
        self._py_hist_append('    env=os.environ,')
        if len(encoding_params_str) > 0:
            self._py_hist_append('    {},'.format(encoding_params_str))
        if len(kwargs) > 0:
            self._py_hist_append('    {},'.format(kwargs_str))
        self._py_hist_append(')')
        self._py_hist_append('')

        if self.stdin_key:
            self._py_hist_append('p.stdin.writelines({})'.format(stdin_list))
            self._py_hist_append('p.stdin.close()')

        self._py_hist_append('ret = p.wait()')
        ### DONE WRITING PY_HIST

        self.logger.info("Evoking subprocess.Popen with...")
        self.logger.info("    args            '{}'.".format(self.args))
        self.logger.info("    ENCODING_PARAMS '{}'.".format(ENCODING_PARAMS))
        self.logger.info("    kwargs          '{}'.".format(kwargs))

        p = subprocess.Popen(
            self.args, stdin=stdin,
            stdout=stdout, stderr=stderr, env=os.environ,
            **ENCODING_PARAMS, **kwargs)

        self.logger.info("Evoked process with...")
        self.logger.info("    args         '{}'.".format(p.args))
        self.logger.info("    PID          '{}'.".format(p.pid))
        self.logger.info("    type(stdin)  '{}'.".format(type(p.stdin)))
        self.logger.info("    type(stdout) '{}'.".format(type(p.stdout)))
        self.logger.info("    type(stderr) '{}'.".format(type(p.stderr)))

        threads = []

        out_streams = []
        out_streams.append(sys.stdout)  # per default to sys.stdout
        threads.append(tee(p.stdout, *out_streams))

        err_streams = []
        err_streams.append(sys.stderr)  # per default to sys.stderr
        threads.append(tee(p.stderr, *err_streams))

        # send stdin if desired and wait for subprocess to complete
        if self.stdin_key:
            p.stdin.writelines(stdin_list)

        try:
            p.stdin.close()
        except:
            pass

        for thread in threads:
            thread.join()  # wait for stream tee threads
        # tee threads close stderr and stdout

        returncode = p.wait()
        self.logger.info(
            "Process returned '{}'.".format(returncode))

        output = {}

        output['returncode'] = returncode

        if self.defuse_bad_rc and returncode != 0:
            fwaction = FWAction(stored_data=output, defuse_children=True)
        elif self.fizzle_bad_rc and returncode != 0:
            raise RuntimeError(
                'CmdTask fizzled! Return code: {}, output: {}'
                .format(returncode, output))
        else:  # returned as expected with 0 return code
            fwaction = FWAction(stored_data=output)

        return fwaction

    def _load_params(self, d):
        """Load parameters from task specs into attributes."""
        EnvTask._load_params(self, d)

        if d.get('stdin_file') and d.get('stdin_key'):
            raise ValueError('CmdTask cannot process both a key and file as the standard in!')

        self.cmd = d.get('cmd')

        # command line options
        opt = d.get('opt', None)
        # opt may be mixed list of [str,dict]
        if (opt is not None) and (not isinstance(opt, list)):
            opt = [opt]  # no check on str or dict elements!

        self.opt = opt

        self.stdin_file = d.get('stdin_file')
        self.stdin_key = d.get('stdin_key')

        self.shell_exe = d.get('shell_exe')
        self.use_shell = d.get('use_shell')
        self.defuse_bad_rc = d.get('defuse_bad_rc')
        self.fizzle_bad_rc = d.get('fizzle_bad_rc', not self.defuse_bad_rc)

        if self.defuse_bad_rc and self.fizzle_bad_rc:
            raise ValueError(
                'CmdTask cannot both FIZZLE and DEFUSE a bad returncode!')


class PyEnvTask(EnvTask, PyTask):
    """Same as PyTask, but allows to modify environment and inject snippets.

    First, performs environment lookup in worker file by `env` (see CmdTask).
    Second, runs python code lines in `init` before calling `func`.
    Offers same logging functionalyte as CmdTask. The optional inputs
    and outputs lists may contain spec keys to add to args list and to make
    the function output available in the curent and in children fireworks.
    Required parameters:
        - func (str): Fully qualified python method. E.g., json.dump, or shutil
            .copy, or some other function that is not part of the standard
            library!
    Optional parameters:
        - init: (str or [str]): Python code to execute before calling `func`,
            i.e. function definitions or imports. May access `fw_spec`.
        - args (list): List of args. Default is empty.
        - kwargs (dict): Dictionary of keyword args. Default is empty.
        - auto_kwargs (bool): If True, all other params not starting with '_'
            are supplied as keyword args.
        - kwargs_inputs (dict): dict of keyword argument names and
            possibly nested fw_spec keys for looking up inputs dynamically.
            Relates to `kwargs` like `inputs` relates to `args`, but the
            generated keyword arguments dict will override static key - value
            pairs in kwargs. fwspec keys can be '.' or '->'-delimited key
            to nested fields.
        - stored_data_varname (str): Whether to store the output in FWAction.
            If this is a string that does not evaluate to False, the output of
            the function will be stored as
            FWAction(stored_data={stored_data_varname: output}). The name is
            deliberately long to avoid potential name conflicts.
        - inputs ([str]): a list of keys in spec which will be used as inputs;
            the generated arguments list will be appended to args.
            Can be key '.' or '->'-delimited key to nested fields.
        - outputs ([str]): a list of spec keys that will be used to pass
            the function's outputs to child fireworks.
        - env (str): allows to specify an environment possibly defined in the
            worker file. If so, additional environment-related intialization
            carried out (see CmdTask).
        - store_stdout (default:False) - store the entire standard output in
            the Firework Launch object's stored_data.
        - stdout_file - (default:None) - store the entire standard output in
            this filepath. If None, the standard out will be streamed to
            sys.stdout.
        - store_stderr - (default:False) - store the entire standard error in
            the Firework Launch object's stored_data.
        - stderr_file - (default:None) - store the entire standard error in
            this filepath. If None, the standard error will be streamed to
            sys.stderr.
        - chunk_number (int): a serial number of the Firetask within a
            group of Firetasks generated by a ForeachTask.
        - propagate (bool, default:None): if True, then set the
            FWAction 'propagate' flag and propagate outputs not only to
            direct children, but all descendants down to the wokflow's leaves.
            If set, then any 'propagate' flag possibly set by an evoked
            function that returns FWAction.

    Examples:
        In order to quickly use a python snippet that we cannot expect to
        be available as a fully qualified function at the worker, we could do

        >>> def get_element_name(n):
        >>>     import ase.data
        >>>     return ase.data.atomic_names[n]
        >>>
        >>> import dill
        >>> func_str = dill.dumps(get_element_name)
        >>> init_lst = [
        >>>     'import builtins, dill',
        >>>     'builtins.injected_func = dill.loads({})'.format(func_str) ]
        >>>
        >>> ft = PyEnvTask(init=init_lst, func='injected_func',
        >>>                args=[18], outputs=['element_name'])
        >>>

        as long as at least 'dill' is available at the worker. The result is

        >>> fw_action = ft.run_task({})
        >>> print(fw_action.as_dict())
        {'stored_data': {}, 'exit': False,
            'update_spec': {'element_name': 'Argon'}, 'mod_spec': [],
            'additions': [], 'detours': [], 'defuse_children': False,
            'defuse_workflow': False}

        The injection into 'builtin' is necessary as evaluating 'init' and
        evoking 'func' won't happen within the same local scope.

        In order to make some Python package available that requires
        the loading of an environmnet module, the environment-specific
        snippet can be stored within a worker file, i.e.

          name:     juwels_noqueue_worker
          category: [ 'juwels_noqueue' ]
          query:    '{}'
          env:
            modified_py_env:
              init:
              - 'import site, sys, os, importlib, builtins'
              - 'sys.path.insert(0, os.path.join(os.environ["MODULESHOME"], "init"))'
              - 'builtins.module = importlib.import_module("env_modules_python").module'
              - 'module("purge")'
              - 'module("use",os.path.join(os.environ["PROJECT"],"common","juwels","easybuild","otherstages"))'
              - 'module("load","Stages/2019a","Intel/2019.3.199-GCC-8.3.0","IntelMPI/2019.3.199")'
              - 'module("load","ASE/3.17.0-Python-3.6.8")'
              - 'module("load","imteksimcs/devel-local-Python-3.6.8")'
              - 'module("load","imteksimpyenv/devel-Python-3.6.8")'
              - 'for d in list(set(os.environ["PYTHONPATH"].split(":")) - set(sys.path)): site.addsitedir(d)'

        then looked up and executed by PyEnvTask before the actual call to 'func'
        if the task's parameter 'env' points to 'mod_py_env'.
    """
    # I would prefer a plain-text injection, but 'dill' does the job.

    _fw_name = 'PyEnvTask'
    required_params = ['func']
    other_params = [
        # base class EnvTask params
        *EnvTask.other_params,
        *PyTask.other_params,
        'kwargs_inputs',
        # PyEnvTask params
        'init'
    ]
    # TODO: remove 'other_params' to make this comment valid again:
    # note that we are not using "optional_params" because we do not want to do
    # strict parameter checking in FireTaskBase due to "auto_kwargs" option

    def _get_func(self, fw_spec):
        """Get function from string."""
        # self.logger.info("type('func') == str.")
        toks = self['func'].rsplit('.', 1)

        if len(toks) == 2:
            modname, funcname = toks
            self._py_hist_append('from {} import {} as func'
                .format(modname, funcname))
            self.logger.info(
                "'func' is a fully qualified name, 'from {} import {}'"
                    .format(modname, funcname))
            mod = __import__(modname, globals(), locals(), [str(funcname)], 0)
            func = getattr(mod, funcname)
        else:
            # Handle built in functions.
            self._py_hist_append('func = {}'.format(toks[0]))
            self.logger.info(
                "'func' is an unqualified name '{}', call directly."
                    .format(toks[0]))
            func = getattr(builtins, toks[0])
        return func

    @trace_method
    def _run_task_internal(self, fw_spec):
        # run snippet
        # in case of a specified worker environment
        if self.env and "_fw_env" in fw_spec \
                and self.env in fw_spec["_fw_env"]:
            self.logger.info("Found {:s}-specific block '{}' within worker file."
                .format(self.env, fw_spec["_fw_env"]))

            # """Parse global init block."""
            # _fw_env : env : init may provide a list of python commans
            # to run, i.e. for module env initialization
            if "init" in fw_spec["_fw_env"][self.env]:
                init = fw_spec["_fw_env"][self.env]["init"]
                if isinstance(init, str):
                    init = [init]
                assert isinstance(init, list)
                for cmd in init:
                    self.logger.info("Execute '{:s}'.".format(cmd))
                    self._py_hist_append(cmd)
                    exec(cmd)

            # """Parse global envronment block."""
            # per default, process inherits current environment
            # self.modenv = os.environ.copy()
            # modify environment before call if desired
            if "env" in fw_spec["_fw_env"][self.env]:
                env_dict = fw_spec["_fw_env"][self.env]["env"]
                if not isinstance(env_dict, dict):
                    raise ValueError(
                        "type({}) = {} of 'env' not accepted, must be dict!"
                            .format(env_dict, type(env_dict)))

                # so far, only simple overrides, no pre- or appending
                for i, (key, value) in enumerate(env_dict.items()):
                    self.logger.info("Set env var '{:s}' = '{:s}'.".format(
                        key, value))
                    self._py_hist_append('os.environ["{:s}"] = "{:s}"'.format(
                        str(key), str(value)))
                    os.environ[str(key)] = str(value)

        # """Parse task-internal preceding code block."""
        if self.init:
            init = self.init
            if isinstance(init, str):
                init = [init]
            assert isinstance(init, list)
            for cmd in init:
                self.logger.info("Execute '{:s}'.".format(cmd))
                self._py_hist_append(cmd)
                exec(cmd)

        func = self._get_func(fw_spec)

        assert callable(func), ("Evaluated 'func' is {}, must be 'callable'"
            .format(type(func)))

        args = list(self.get('args', []))  # defensive copy

        self.logger.info("'args = {}'".format(args))

        inputs = self.get('inputs', [])
        self.logger.info("'inputs = {}'".format(inputs))

        assert isinstance(inputs, list), "'inputs' must be list."
        for item in inputs:
            args.append(get_nested_dict_value(fw_spec, item))

        if self.get('auto_kwargs'):
            kwargs = {k: v for k, v in self.items()
                      if not (k.startswith('_')
                              or k in self.required_params
                              or k in self.other_params)}
        else:
            kwargs = self.get('kwargs', {})

        self.logger.info("'kwargs = {}'".format(kwargs))

        kwargs_inputs = self.get('kwargs_inputs', {})
        self.logger.info("'kwargs_inputs = {}'".format(kwargs_inputs))
        assert isinstance(kwargs_inputs, dict), "'kwargs_inputs' must be dict."
        for kwarg_name, kwarg_key in kwargs_inputs.items():
            kwargs[kwarg_name] = get_nested_dict_value(fw_spec, kwarg_key)

        if len(args) > 0:
            self._py_hist_append('args = {}'.format(args))
        else:
            self._py_hist_append('args = []')

        if len(kwargs) > 0:
            self._py_hist_append('kwargs = {}'.format(kwargs))
        else:
            self._py_hist_append('kwargs = {}')

        self._py_hist_append('output = func(*args, **kwargs)')

        output = func(*args, **kwargs)

        if isinstance(output, FWAction):
            self.logger.info("'type(output) == FWAction', return directly.")
            return output

        self.logger.info("'type(output) == {}', build FWAction."
            .format(type(output)))
        actions = {}
        outputs = self.get('outputs', [])
        assert isinstance(outputs, list)
        if len(outputs) == 1:
            if self.get('chunk_number') is None:
                # actions['update_spec'] = {outputs[0]: output}
                actions['mod_spec'] = [{'_set': {outputs[0]: output}}]
            else:
                if isinstance(output, (list, tuple, set)):
                    mod_spec = [{'_push': {outputs[0]: i}} for i in output]
                else:
                    mod_spec = [{'_push': {outputs[0]: output}}]
                actions['mod_spec'] = mod_spec
        elif len(outputs) > 1:
            # assert isinstance(output, (list, tuple, set))  # fails in case of numpy array
            assert len(output) == len(outputs) # implies above assertion and should incorporate more iterable types
            # actions['update_spec'] = dict(zip(outputs, output))
            actions['mod_spec'] = [{'_set': dict(zip(outputs, output))}]

        if self.get('stored_data_varname'):
            actions['stored_data'] = {self['stored_data_varname']: output}
        if len(actions) > 0:
            self.logger.info("Built actions '{}.'".format(actions))
            return FWAction(**actions)

    def _load_params(self, d):
        """Load parameters from task specs into attributes."""
        EnvTask._load_params(self, d)
        # PyTask does not need to load params (?)
        self.init = self.get('init', None)


class PickledPyEnvTask(PyEnvTask):
    """Same as PyEnvTask, but expects pickled function instead of function name.

    As bytes might actually be stored as their bytes literals string
    representation within FireWork's, the task will try to eval 'func'
    before unpickling it if its type is str and not bytes.

    Examples:
        The shorter equivalent to above's PyEnvTask example looks likes this:

        >>> func_str = dill.dumps(get_element_name)
        >>> ft = PyEnvTask(func=func_str, args=[18], outputs=['element_name'])

        Of course, all references within the pickled object must be available
        at execution time. If the function has been pickled with 'dill',
        then 'dill' has to be available when unpickling.

        If 'py_hist_file' has been specified, then this task will produce
        a simple python file with the purpose to
        rerun the the call quickly outside of the FireWorks framework:

        >>> import pickle
        >>> func = pickle.loads(b'SOME_PICKLED_SEQUENCE')
        >>> args = [18]
        >>> kwargs = {}
        >>> output = func(*args, **kwargs)
    """

    _fw_name = 'PickledPyEnvTask'

    # try to unpickle with dill if 'func' is bytes
    def _get_func(self, fw_spec):
        """Get function from pickled bytes."""

        self._py_hist_append('import pickle')
        func_bytes = self['func']
        if isinstance(func_bytes, str):
            self.logger.info("type('func') == str, convert to bytes.")
            # when serializing a task, FireWorks apparently
            # puts the string representation
            # of the bytes objetc into the database
            func_bytes = eval(func_bytes)
        self._py_hist_append('func = pickle.loads({})'.format(func_bytes))
        func = pickle.loads(func_bytes)
        return func


class EvalPyEnvTask(PyEnvTask):
    """Same as PyEnvTask, but expects a lambda definition as string.

    Examples:

        >>> func_str = 'lambda x, y: x+y'
        >>> ft = EvalPyEnvTask(func=func_str, args=[1,2], outputs=['sum'])
        >>> fw_action = ft.run_task()

        returns an FWAction containing {'mod_spec': [{'_set': {'sum': 3}}]}.
    """

    _fw_name = 'EvalPyEnvTask'

    def _get_func(self, fw_spec):
        """Get function from string evaluation."""
        func_str = self['func']
        self._py_hist_append("func = eval('{:s}')".format(func_str))
        func = eval(func_str)
        return func
