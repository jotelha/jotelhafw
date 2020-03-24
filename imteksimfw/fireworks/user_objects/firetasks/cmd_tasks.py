#!/usr/bin/env python
"""Enhanced script task."""

import logging
import io
import os
import subprocess
import sys
import threading

# fireworks-internal
from fireworks.core.firework import FWAction
from fireworks.user_objects.firetasks.script_task import ScriptTask

# in order to have a somewhat centralized encoding configuration
from fireworks.utilities.fw_serializers import ENCODING_PARAMS

if sys.version_info[0] > 2:
    basestring = str


__author__ = 'Johannes Hoermann'
__copyright__ = 'Copyright 2018, IMTEK'
__version__ = '0.1.1'
__maintainer__ = 'Johannes Hoermann'
__email__ = 'johannes.hoermann@imtek.uni-freiburg.de'
__date__ = 'Mar 18, 2020'


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
    """Preserve original os.environ."""
    def __enter__(self):
        self._original_environ = os.environ.copy()

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.environ = self._original_environ


class CmdTask(ScriptTask):
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
        - stdin_key - (default:None) - feed this String as standard input
            to the script
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

    required_params = ['cmd']
    optional_params = [
        'opt', 'env', 'loglevel',
        'stdin_file', 'stdin_key',
        'stdout_file', 'stderr_file', 'store_stdout', 'store_stderr',
        'use_shell', 'shell_exe', 'defuse_bad_rc', 'fizzle_bad_rc']
    _fw_name = 'CmdTask'

    @property
    def args(self):
        return [a if isinstance(a, basestring) else str(a) for a in self._args]

    def _parse_global_init_block(self, fw_spec):
        """Parse global init block."""
        # _fw_env : env : init may provide a list of python commans
        # to run, i.e. for module env initialization
        if "init" in fw_spec["_fw_env"][self.env]:
            init = fw_spec["_fw_env"][self.env]["init"]
            if isinstance(init, basestring):
                init = [init]
            assert isinstance(init, list)
            for cmd in init:
                self.logger.info("Execute '{:s}'.".format(cmd))
                exec(cmd)
        else:
            pass  # no particular initialization for this environment
        return os.environ

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
            for i, (key, value) in enumerate(env_dict):
                self.logger.info("Set env var '{:s}' = '{:s}'.".format(
                    key, value))
            os.environ[str(key)] = str(value)
        # return os.environ

    def _parse_cmd_init_block(self, fw_spec):
        """Parse per-command init block."""
        cmd_block = fw_spec["_fw_env"][self.env]["cmd"][self.cmd]
        if "init" in cmd_block:
            init = cmd_block["init"]
            if isinstance(init, basestring):
                init = [init]
            assert isinstance(init, list), "'init' must be str or list"
            for cmd in init:
                self.logger.info("Execute '{:s}'.".format(cmd))
                exec(cmd)
        else:
            pass  # no specific initialization for this command
        # return os.environ

    def _parse_cmd_substitute_block(self, fw_spec):
        """Parse per-command substitute block."""
        cmd_block = fw_spec["_fw_env"][self.env]["cmd"][self.cmd]
        if "substitute" in cmd_block:
            substitute = cmd_block["substitute"]
            assert isinstance(substitute, basestring), "substitute must be str"
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
                        if isinstance(processed_prefix, basestring):
                            processed_prefix = processed_prefix.split()
                        else:
                            raise ValueError(
                                "Output {} of prefix #{} evaluation not accepted!".format(
                                    processed_prefix, i))
                    else:
                        raise ValueError(
                            "Formatting {} of prefix #{} not accepted!".format(
                                prefix, i))
                elif isinstance(prefix, basestring):
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
        """Parse command-specific envronment block."""
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
            for i, (key, value) in enumerate(env_dict):
                self.logger.info("Set env var '{:s}' = '{:s}'.".format(
                    key, value))
            os.environ[str(key)] = str(value)

    def _parse_cmd_block(self, fw_spec):
        """Parse command-specific environment block."""
        if "cmd" in fw_spec["_fw_env"][self.env] \
                and self.cmd in fw_spec["_fw_env"][self.env]["cmd"]:
            self.logger.info("Found {:s}-specific block '{}' within worker file."
                .format(self.cmd, fw_spec["_fw_env"][self.env]["cmd"]))
            # same as above, evaluate command-specific initialization code
            self._parse_cmd_init_block(fw_spec)
            self._parse_cmd_substitute_block(fw_spec)
            self._parse_cmd_prefix_block(fw_spec)
            self._parse_cmd_env_block(fw_spec)
        else:  # no command-specific expansion in environment, use as is
            self._args.append(self.cmd)
        # return os.environ

    def _parse_args(self, fw_spec):
        self._args = []
        # in case of a specified worker environment
        if self.env and "_fw_env" in fw_spec \
                and self.env in fw_spec["_fw_env"]:
            self.logger.info("Found {:s}-specific block '{}' within worker file."
                .format(self.env, fw_spec["_fw_env"]))

            self._parse_global_init_block(fw_spec)
            self._parse_cmd_env_block(fw_spec)
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

    def _prepare_logger(self, stdout=sys.stdout, stderr=sys.stderr):
      # explicitly modify the root logger (necessary?)
        self.logger = logging.getLogger(self._fw_name)
        self.logger.setLevel(self.loglevel)

        # remove all handlers
        for h in self.logger.handlers:
            self.logger.removeHandler(h)

        # create and append custom handles

        # only info & debug to stdout
        def stdout_filter(record):
            return record.levelno <= logging.INFO

        stdouth = logging.StreamHandler(stdout)
        stdouth.setLevel(self.loglevel)
        stdouth.addFilter(stdout_filter)

        stderrh = logging.StreamHandler(stderr)
        stderrh.setLevel(logging.WARNING)

        self.logger.addHandler(stdouth)
        self.logger.addHandler(stderrh)

        if self.store_stdout:
            outh = logging.StreamHandler(self._stdout)
            outh.setLevel(self.loglevel)
            outh.addFilter(stdout_filter)
            self.logger.addHandler(outh)

        if self.store_stderr:
            errh = logging.StreamHandler(self._stderr)
            errh.setLevel(logging.WARNING)
            self.logger.addHandler(errh)

        if self.stdout_file:
            outfh = logging.FileHandler(self.stdout_file, mode='a+', **ENCODING_PARAMS)
            outfh.setLevel(self.loglevel)
            outfh.addFilter(stdout_filter)
            self.logger.addHandler(outfh)

        if self.stderr_file:
            errfh = logging.FileHandler(self.stderr_file, mode='a+', **ENCODING_PARAMS)
            errfh.setLevel(logging.WARNING)
            self.logger.addHandler(errfh)

    def _run_task_internal(self, fw_spec, stdin):
        """Runs a sub-process"""
        if self.store_stdout:
            self._stdout = io.TextIOWrapper(io.BytesIO(),**ENCODING_PARAMS)

        if self.store_stderr:
            self._stderr = io.TextIOWrapper(io.BytesIO(),**ENCODING_PARAMS)

        self._prepare_logger()

        stdout = subprocess.PIPE if self.store_stdout or self.stdout_file else None
        stderr = subprocess.PIPE if self.store_stderr or self.stderr_file else None

        returncodes = []

        with TemporaryOSEnviron():
            self._parse_args(fw_spec)

            p = subprocess.Popen(
                self.args, executable=self.shell_exe, stdin=stdin,
                stdout=stdout, stderr=stderr,
                shell=self.use_shell, **ENCODING_PARAMS)

            threads = []

            if stdout is not None:
                out_streams = []
                if self.stdout_file:
                    outf = open(self.stdout_file, 'a+', **ENCODING_PARAMS)
                    out_streams.append(outf)
                if self.store_stdout:
                    # outs = io.TextIOWrapper(BytesIO(),**ENCODING_PARAMS)
                    out_streams.append(self._stdout)

                out_streams.append(sys.stdout)  # per default to sys.stdout
                threads.append(tee(p.stdout, *out_streams))

            if stderr is not None:
                err_streams = []
                if self.stderr_file:
                    errf = open(self.stderr_file, 'a+', **ENCODING_PARAMS)
                    err_streams.append(errf)
                if self.store_stderr:
                    # errs = io.TextIOWrapper(BytesIO(),**ENCODING_PARAMS)
                    err_streams.append(self._stderr)

                err_streams.append(sys.stderr)  # per default to sys.stderr
                threads.append(tee(p.stderr, *err_streams))

            # send stdin if desired and wait for subprocess to complete
            if self.stdin_key:
                # if p.stdin and sys.stdin.encoding:
                try:
                    (stdout_data, stderr_data) = p.communicate(
                        fw_spec[self.stdin_key])
                    #    fw_spec[self.stdin_key].encode(**ENCODING_PARAMS))
                except Exception as exc:
                    self.logger.exception(
                        "Communcating 'fw_spec[{:s}] = {:s}' failed with  exception '{}'."
                        .format(self.stdin_key, fw_spec[self.stdin_key], exc))
            else:
                try:
                    (stdout_data, stderr_data) = p.communicate()
                except Exception as exc:
                    self.logger.exception(
                        "Communcating failed with exception '{}'.".format(exc))

            returncodes.append(p.returncode)

            for t in threads: t.join()  # wait for stream tee threads

            # write out the output, error files if specified
            # stdout_str = stdout_data.decode(errors="ignore", **ENCODING_PARAMS) if isinstance(
            #    stdout_data, bytes) else stdout
            # stderr_str = stderr_data.decode(errors="ignore", **ENCODING_PARAMS) if isinstance(
            #    stderr_data, bytes) else stderr_data

        # write the output keys
        output = {}

        if self.store_stdout:
            output['stdout'] = self._stdout.read()

        if self.store_stderr:
            output['stderr'] = self._stderr.read()

        output['returncode'] = returncodes[-1]
        output['all_returncodes'] = returncodes

        if self.defuse_bad_rc and sum(returncodes) != 0:
            fwaction = FWAction(stored_data=output, defuse_children=True)
        elif self.fizzle_bad_rc and sum(returncodes) != 0:
            raise RuntimeError(
                'CmdTask fizzled! Return code: {}'.format(returncodes))
        else:
            fwaction = FWAction(stored_data=output)

        return fwaction

    def _load_params(self, d):
        if d.get('stdin_file') and d.get('stdin_key'):
            raise ValueError('CmdTask cannot process both a key and file as the standard in!')

        self.use_shell = d.get('use_shell', True)

        self.cmd = d.get('cmd')

        # command line options
        opt = d.get('opt', None)
        if isinstance(opt, basestring):
            opt = [opt]
        self.opt = opt
        self.env = self.get('env')
        self.loglevel = self.get('loglevel', logging.DEBUG)

        self.stdin_file = d.get('stdin_file')
        self.stdin_key = d.get('stdin_key')
        self.stdout_file = d.get('stdout_file')
        self.stderr_file = d.get('stderr_file')
        self.store_stdout = d.get('store_stdout')
        self.store_stderr = d.get('store_stderr')
        self.shell_exe = d.get('shell_exe', '/bin/bash')  # bash as default
        self.defuse_bad_rc = d.get('defuse_bad_rc')
        self.fizzle_bad_rc = d.get('fizzle_bad_rc', not self.defuse_bad_rc)

        if self.defuse_bad_rc and self.fizzle_bad_rc:
            raise ValueError(
                'CmdTask cannot both FIZZLE and DEFUSE a bad returncode!')

    # def __init__(self, *args, **kwargs):
        # """Add logger to task."""
        # self.logger = logging.getLogger(__name__)
        # super().__init__(*args, **kwargs)

    @classmethod
    def from_str(cls, shell_cmd, parameters=None):
        parameters = parameters if parameters else {}
        parameters['cmd'] = [shell_cmd]
        parameters['use_shell'] = True
        return cls(parameters)
