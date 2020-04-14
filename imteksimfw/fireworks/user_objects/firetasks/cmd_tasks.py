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
import os
import subprocess
import sys
import threading

import pickle
# try:  # try to unpickle func in PyTask if bytes
#     import dill
# except ModuleNotFoundError:  # py >= 3.6
#     pass

from contextlib import redirect_stdout, redirect_stderr
from six.moves import builtins

# fireworks-internal
from fireworks.core.firework import FiretaskBase, FWAction
from fireworks.user_objects.firetasks.script_task import ScriptTask, PyTask

# in order to have a somewhat centralized encoding configuration
from fireworks.utilities.fw_serializers import ENCODING_PARAMS

from fireworks.utilities.dict_mods import arrow_to_dot

from imteksimfw.fireworks.utilities.tracer import trace_func

__author__ = 'Johannes Hoermann'
__copyright__ = 'Copyright 2018, IMTEK'
__version__ = '0.1.1'
__maintainer__ = 'Johannes Hoermann'
__email__ = 'johannes.hoermann@imtek.uni-freiburg.de'
__date__ = 'Mar 18, 2020'

# TODO: needs "proper" alternative to '_py_hist' print statements

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

class EnvTask(FiretaskBase):
    """Abstract base class for tasks that modify the environmen.

    All derivatives will have the option to
      * specify an environment 'env' to be looked up within the worker file,
      * redirect stdout and stderr streams to files 'stdout_file' and
        'stderr_file',
      * store stdout and stderr in database if 'stored_stdout' or
        'store_stderr' are set,
      * trace execution and stream calls and variable changes to
        'call_log_file' and 'vars_log_file' if the package 'hunter' is
        available.
      * stream a simple (non-exhaustie) command history to a 'py_hist_file'.
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
        'store_stdout',
        'store_stderr',
        'propagate',  # propagate update_spec and mod_spec down all decendants
    ]

    def _py_hist_append(self, line):
        self._py_hist_stream.write(line + '\n')

    # TODO: would like to use these _parse_*** functions
    # as 'macros', i.e. executing everything in their
    # caller's frame, not sure how to.
    def _parse_global_init_block(self, fw_spec):
        """Parse global init block."""
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

    def _parse_global_env_block(self, fw_spec):
        """Parse global envronment block."""
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

    def _parse_global_block(self, fw_spec):
        # in case of a specified worker environment
        if self.env and "_fw_env" in fw_spec \
                and self.env in fw_spec["_fw_env"]:
            self.logger.info("Found {:s}-specific block '{}' within worker file."
                .format(self.env, fw_spec["_fw_env"]))

            self._parse_global_init_block(fw_spec)
            self._parse_global_env_block(fw_spec)

    def _prepare_logger(self, stdout=sys.stdout, stderr=sys.stderr):
        """Prepare log output streams."""
        # explicitly modify the root logger (necessary?)
        self.logger = logging.getLogger(self._fw_name)
        self.logger.setLevel(self.loglevel)

        # remove all handlers
        for h in self.logger.handlers:
            self.logger.removeHandler(h)

        # create and append custom handles

        stderrh = logging.StreamHandler(stderr)
        stderrh.setLevel(self.loglevel)

        self.logger.addHandler(stderrh)

        if self.store_stderr:
            errh = logging.StreamHandler(self._stderr)
            errh.setLevel(self.loglevel)
            self.logger.addHandler(errh)

        if self.stderr_file:
            errfh = logging.FileHandler(self.stderr_file, mode='a', **ENCODING_PARAMS)
            errfh.setLevel(self.loglevel)
            self.logger.addHandler(errfh)

        if self.py_hist_file:
            self._py_hist_stream = open(self.py_hist_file, mode='a', **ENCODING_PARAMS)
        else:
            self._py_hist_stream = os.devnull

        if self.call_log_file:
            self._call_log_stream = open(self.call_log_file, mode='a', **ENCODING_PARAMS)
        else:
            self._call_log_stream = os.devnull

        if self.vars_log_file:
            self._vars_log_stream = open(self.vars_log_file, mode='a', **ENCODING_PARAMS)
        else:
            self._vars_log_stream = os.devnull

    def run_task(self, fw_spec):
        """Run a sub-process. Modify environment if desired."""

        # _history holds python commands in string from to conserve
        self._history = []

        if self.get('use_global_spec'):
            self._load_params(fw_spec)
        else:
            self._load_params(self)

        # create non-default streams to insert into db if desired
        if self.store_stdout:
            self._stdout = io.TextIOWrapper(io.BytesIO(),**ENCODING_PARAMS)
        else:
            self._stdout = sys.stdout

        if self.store_stderr:
            self._stderr = io.TextIOWrapper(io.BytesIO(),**ENCODING_PARAMS)
        else:
            self._stderr = sys.stderr

        # log messages std streams are logged to files and database,
        # depending on flags.
        self._prepare_logger()

        # TODO: redirect of stdout and stderr won't tee into file if desired
        # Pull 'tee' functionality of CmdTask one level up here.
        with TemporaryOSEnviron(), TemporarySysPath(), \
                redirect_stdout(self._stdout), redirect_stderr(self._stderr):
            ret = self._run_task_internal(fw_spec)

        if isinstance(ret, FWAction):
            fw_action = ret
        else:
            fw_action = FWAction()

        output = {}
        if self.store_stdout:
            self._stdout.seek(0)
            output['stdout'] = self._stdout.read()

        if self.store_stderr:
            self._stderr.seek(0)
            output['stderr'] = self._stderr.read()

        fw_action.stored_data.update(output)

        # 'propagate' only development feature for now
        if hasattr(fw_action, 'propagate') and self.propagate is not None:
            fw_action.propagate = self.propagate

        return fw_action

    # TODO: get rid of those _load_params functions (relic from ScriptTask)
    def _load_params(self, d):
        self.env = self.get('env')
        self.loglevel = self.get('loglevel', logging.DEBUG)
        self.py_hist_file = self.get('py_hist_file', 'simple_hist.py')
        self.call_log_file = self.get('call_log_file', 'calls.log')
        self.vars_log_file = self.get('vars_log_file', 'vars.log')

        self.stdout_file = d.get('stdout_file')
        self.stderr_file = d.get('stderr_file')
        self.store_stdout = d.get('store_stdout')
        self.store_stderr = d.get('store_stderr')

        self.propagate = d.get('propagate')


class CmdTask(EnvTask, ScriptTask):
    """Enhanced script task, runs (possibly environment dependent)  command.

    Required params:
        - cmd (str): command to look up within '_fw_env (see below for details)

    Optional params:
        - opt ([obj]): list of strings (or int, float, ...) to pass as
            options / arguments tp 'cmd'
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

    def _parse_cmd_init_block(self, fw_spec):
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

    def _parse_cmd_substitute_block(self, fw_spec):
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

    def _parse_cmd_prefix_block(self, fw_spec):
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

    def _parse_cmd_env_block(self, fw_spec):
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

    def _parse_cmd_block(self, fw_spec):
        """Parse command-specific environment block."""
        if "cmd" in fw_spec["_fw_env"][self.env] \
                and self.cmd in fw_spec["_fw_env"][self.env]["cmd"]:
            self.logger.info("Found {:s}-specific block '{}' within worker file."
                .format(self.cmd, fw_spec["_fw_env"][self.env]["cmd"][self.cmd]))
            # same as above, evaluate command-specific initialization code
            self._parse_cmd_init_block(fw_spec)
            self._parse_cmd_substitute_block(fw_spec)
            self._parse_cmd_prefix_block(fw_spec)
            self._parse_cmd_env_block(fw_spec)
        else:  # no command-specific expansion in environment, use as is
            self._args.append(self.cmd)

    def _parse_args(self, fw_spec):
        self._args = []
        # in case of a specified worker environment
        self._parse_global_block()
        if self.env and "_fw_env" in fw_spec \
                and self.env in fw_spec["_fw_env"]:
            # check whether there is any machine-specific "expansion" for
            # the command head
            self._parse_cmd_block(fw_spec)
        elif "_fw_env" in fw_spec and self.cmd in fw_spec["_fw_env"]:
            # check whether there is any desired command and whether there
            # exists a machine-specific "alias"
            self.logger.info("Found root-level {:s}-specific block '{}' within worker file."
                .format(self.cmd, fw_spec["_fw_env"][self.cmd]))
            self._args = [fw_spec["_fw_env"][self.cmd]]
        else:
            self._args = [self.cmd]

        if self.opt:  # extend by command line options if available
            self._args.extend(self.opt)

        self.logger.info("Built args '{}'.".format(self._args))
        self.logger.info("Built command '{:s}'.".format(
            ' '.join(self.args)))
        self.logger.debug("Process environment '{}'.".format(os.environ))

    @trace_method
    def _run_task_internal(self, fw_spec):
        """Run task."""
        stdout = subprocess.PIPE if self.store_stdout or self.stdout_file else None
        stderr = subprocess.PIPE if self.store_stderr or self.stderr_file else None

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

        self._parse_args(fw_spec)

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

        self._py_hist_append('# os.environ = {}\n'.format(dict(os.environ)))

        stdoutstr = 'subprocess.PIPE' if self.store_stdout or self.stdout_file else 'None'
        stderrstr = 'subprocess.PIPE' if self.store_stderr or self.stderr_file else 'None'
        if self.stdin_key:
            stdinstr = 'subprocess.PIPE'
        elif self.stdin_file:
            stdinstr = 'open({:s}, "r", "{:s}")'.format(
                self.stdin_file, encoding_params_str)
        else:
            stdinstr = 'None'

        self._py_hist_append('\n')
        self._py_hist_append('p = subprocess.Popen(\n')
        self._py_hist_append('    {},\n'.format(self.args))
        self._py_hist_append('    stdin={},\n'.format(stdinstr))
        self._py_hist_append('    stdout={},\n'.format(stdoutstr))
        self._py_hist_append('    stderr={},\n'.format(stderrstr))
        self._py_hist_append('    env=os.environ')
        if len(encoding_params_str) > 0:
            self._py_hist_append(',\n    {}'.format(encoding_params_str))
        if len(kwargs) > 0:
            self._py_hist_append(',\n    {}'.format(kwargs_str))
        self._py_hist_append(')\n\n')

        if self.stdin_key:
            self._py_hist_append('p.stdin.writelines({})\n'.format(stdin_list))
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

        if stdout is not None:
            out_streams = []
            if self.stdout_file:
                self.logger.info("Redirecting process stdout to '{:s}'."
                    .format(self.stdout_file))
                outf = open(self.stdout_file, 'a', **ENCODING_PARAMS)
                out_streams.append(outf)
            if self.store_stdout:
                self.logger.info("Redirecting process stdout to internal buffer '{}'."
                    .format(type(self._stdout)))
                # outs = io.TextIOWrapper(BytesIO(),**ENCODING_PARAMS)
                out_streams.append(self._stdout)

            out_streams.append(sys.stdout)  # per default to sys.stdout
            threads.append(tee(p.stdout, *out_streams))

        if stderr is not None:
            err_streams = []
            if self.stderr_file:
                self.logger.info("Redirecting process stderr to '{:s}'."
                    .format(self.stderr_file))
                errf = open(self.stderr_file, 'a', **ENCODING_PARAMS)
                err_streams.append(errf)
            if self.store_stderr:
                self.logger.info("Redirecting process stderr to internal buffer '{}'."
                    .format(type(self._stderr)))
                # errs = io.TextIOWrapper(BytesIO(),**ENCODING_PARAMS)
                err_streams.append(self._stderr)

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
        if isinstance(opt, str):
            opt = [opt]
        if opt:
            assert isinstance(opt, list), "opt must be str or list of str-like!"

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

    def _parse_local_init_block(self, fw_spec):
        """Parse task-internal preceding code block."""
        # _fw_env : env : init may provide a list of python commans
        # to run, i.e. for module env initialization
        if self.init:
            init = self.init
            if isinstance(init, str):
                init = [init]
            assert isinstance(init, list)
            for cmd in init:
                self.logger.info("Execute '{:s}'.".format(cmd))
                self._py_hist_append(cmd)
                exec(cmd)

    @trace_method
    def _run_task_internal(self, fw_spec):
        # run snippet
        self._parse_global_block(fw_spec)
        self._parse_local_init_block(fw_spec)

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
            self._py_hist_append('kwargs = {}'.format(args))
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
            assert isinstance(output, (list, tuple, set))  # fails in case of numpy array
            assert len(output) == len(outputs)
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
