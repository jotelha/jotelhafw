#!/usr/bin/env python3
"""Manages FireWorks rocket launchers and associated scripts as daemons"""

import os
import signal  # for unix system signal treatment (https://people.cs.pitt.edu/~alanjawi/cs449/code/shell/UnixSignals.htm)
import sys  # for stdout and stderr
import daemon  # for detached daemons
import datetime  # for generating timestamps
import getpass # get username
import logging
import monty.serialization  # for reading config files
import pid # for pidfiles
import psutil  # checking process status
# import pidfile
import socket  # for host name
import subprocess

# configuration handling modeled following
# https://github.com/materialsproject/fireworks/blob/master/fireworks/fw_config.py

FWRLM_CONFIG_FILE_DIR = '.'
FWRLM_CONFIG_FILE_NAME = 'FWRLM_config.yaml'
FWRLM_CONFIG_FILE_ENV_VAR = 'FWRLM_CONFIG_FILE'

FW_CONFIG_PREFIX = os.path.join(os.path.expanduser('~'), ".fireworks")
FW_CONFIG_FILE_NAME = "FW_config.yaml"
FW_AUTH_FILE_NAME = "fireworks_mongodb_auth.yaml"

LAUNCHPAD_LOC = os.path.join(os.path.expanduser('~'), "fw_launchpar")
LOGDIR_LOC = os.path.join(os.path.expanduser('~'), "fw_logdir")

# allow multiple rlaunch processes
MULTI_RLAUNCH_NTASKS = 0
OMP_NUM_THREADS = 1

# gui settings
FWGUI_PORT = 19886

# mongodb and ssh tunnel settings
MONGODB_HOST = 'localhost'
MONGODB_PORT_REMOTE = 27017
MONGODB_PORT_LOCAL = 27037
FIREWORKS_DB = 'fireworks'
FIREWORKS_USER = 'fireworks'
FIREWORKS_PWD = 'fireworks'
SSH_HOST = '132.230.102.164'
SSH_USER = 'sshclient'
SSH_TUNNEL = False
SSH_KEY = os.path.join(os.path.expanduser('~'), ".ssh", "id_rsa")
USE_RSTUNNEL = True
RSTUNNEL_CONFIG = None # path to rstunnel config file

# run daemon to periodically check offline runs
RECOVER_OFFLINE = True

# MACHINE-specfific settings
MACHINE = "JUWELS"

RLAUNCH_FWORKER_FILE = None
# if not set explicitl, then stick to automatic convention
# "${FW_CONFIG_PREFIX}/${MACHINE:lowercase}_noqueue_worker.yaml"

QLAUNCH_FWORKER_FILE = None
# if not set explicitl, then stick to automatic convention
# "${FW_CONFIG_PREFIX}/${MACHINE:lowercase}_queue_offline_worker.yaml"

QADAPTER_FILE = None
# if not set explicitl, then stick to automatic convention
# "${FW_CONFIG_PREFIX}/${MACHINE:lowercase}_{SCHEDULER_lowercase}_qadapter_offline.yaml"

SCHEDULER = 'SLURM'

# for python-daemon intro, refer to
# https://dpbl.wordpress.com/2017/02/12/a-tutorial-on-python-daemon/ and
# https://www.python.org/dev/peps/pep-3143/

# class PidFile(pid.PidFile):
#     """Allows checking of PID without setting up"""
#     def check(self):
#         if not self._is_setup:
#             # set up light, only file name, no pid, no registration ...
#             self.filename = self._make_filename()
#
#         self.logger.debug("%r check pidfile: %s", self, self.filename)
#
#         if self.fh is None:
#             if self.filename and os.path.isfile(self.filename):
#                 with open(self.filename, "r") as fh:
#                     return self._inner_check(fh)
#             return PID_CHECK_NOFILE
#
#         return self._inner_check(self.fh)

class FireWorksRocketLauncherManager():
    @property
    def fw_config_prefix(self):
        return FW_CONFIG_PREFIX

    @property
    def launchpad_loc(self):
        return LAUNCHPAD_LOC

    @property
    def logdir_loc(self):
        return LOGDIR_LOC

    @property
    def fw_auth_file_path(self):
        return os.path.join(FW_CONFIG_PREFIX, FW_AUTH_FILE_NAME)

    @property
    def fw_config_file_path(self):
        return os.path.join(FW_CONFIG_PREFIX, FW_CONFIG_FILE_NAME)

    @property
    def machine(self):
        return MACHINE

    @property
    def scheduler(self):
        return SCHEDULER

    @property
    def rlaunch_fworker_file(self):
        return RLAUNCH_FWORKER_FILE if RLAUNCH_FWORKER_FILE else os.path.join(
            FW_CONFIG_PREFIX, "{:s}_noqueue_worker.yaml"
                .format(self.machine.lower()))

    @property
    def qlaunch_fworker_file(self):
        return QLAUNCH_FWORKER_FILE if QLAUNCH_FWORKER_FILE else os.path.join(
            FW_CONFIG_PREFIX, "{:s}_queue_offline_worker.yaml"
                .format(self.machine.lower()))

    @property
    def qadapter_file(self):
        return QADAPTER_FILE if QADAPTER_FILE else os.path.join(
            FW_CONFIG_PREFIX, "{:s}_{:s}_qadapter_offline.yaml"
                .format(self.machine.lower(), self.scheduler.lower()))

    @property
    def rlaunch_interval(self):
        return 10  # seconds

    @property
    def timestamp(self):
        return self._launchtime.strftime('%Y%m%d%H%M%S%f')

    # @property
    # def pidfile(self):
    #    """pid.PidFile for daemon"""
    #    if not hasattr(self, '_pidfile'):
    #        self._pidfile = PidFile(self.pidfile_name)
    #    return self._pidfile

    @property
    def piddir(self):
        if not hasattr(self, '_piddir'):
            self._piddir = pid.utils.determine_pid_directory()
        return self._piddir

    @property
    def process_name(self):
        return self._command_line[0]

    @property
    def command_line(self):
        if not hasattr(self, '_command_line'):
            self._command_line = psutil.Process(os.getpid()).cmdline()
        return self._command_line

    @property
    def outfile(self):
        """File descriptor for stdout log file"""
        if not hasattr(self, '_outfile'):
            self._outfile = open(self.outfile_name,'w+')
        return self._outfile

    @property
    def errfile(self):
        """File descriptor for stderr log file"""
        if not hasattr(self, '_errfile'):
            self._errfile = open(self.errfile_name,'w+')
        return self._errfile

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.init()

    def init(self):
        self._launchtime = datetime.datetime.now()
        self.module_dir = os.path.dirname(os.path.abspath(__file__))
        self.root_dir = os.path.dirname(self.module_dir)  # FW root dir

        config_paths = []

        test_paths = [
            os.getcwd(),
            os.path.join(os.path.expanduser('~'), ".fireworks"),
            self.root_dir,
        ]

        for p in test_paths:
            fp = os.path.join(p, FWRLM_CONFIG_FILE_NAME)
            if fp not in config_paths and os.path.exists(fp):
                config_paths.append(fp)

        if FWRLM_CONFIG_FILE_ENV_VAR in os.environ \
            and os.environ[FWRLM_CONFIG_FILE_ENV_VAR] not in config_paths:
            config_paths.append(os.environ[FWRLM_CONFIG_FILE_ENV_VAR])


        if len(config_paths) > 1:
            self.logger.warn("Found many potential paths for {}: {}"
                .format(FWRLM_CONFIG_FILE_NAME, config_paths))
            self.logger.warn("Choosing as default: {}"
                .format(config_paths[0]))

        if len(config_paths) > 0 and os.path.exists(config_paths[0]):
            overrides = monty.serialization.loadfn(config_paths[0])
            for key, v in overrides.items():
                if key not in globals():
                    raise ValueError(
                        'Invalid FWRLM_config file has unknown parameter: {}'
                            .format(key))
                else:
                    self.logger.info("Set key : value pair '{}' : '{}'"
                        .format(key, v))
                    globals()[key] = v

    def shutdown(self):
        """Shut down daemon neatly"""
        sys.exit(0)

    # def spawn_rlaunch_daemon(self):
    #    self.spawn_daemon(self.spawn_rlaunch, self.rlaunch_pidfile)

    # def spawn_dummy_daemon(self):
    #    self.spawn_daemon(
    #        self.spawn_dummy, self.dummy_pidfile,
    #        open(self.dummy_outfile,'w+'), open(self.dummy_errfile,'w+'))

    # inspired by https://github.com/mosquito/python-pidfile/blob/d2ac227a5c3635db2541aa8d8e4507bdeb4b34e2/pidfile/pidfile.py#L15
    def check_daemon(self, raise_exc=True):
        """Check for running process"""
        pidfile_path = os.path.join(self.piddir,self.pidfile_name)
        if not os.path.exists(pidfile_path):
            self.logger.debug("{} does not exist.".format(pidfile_path))
            return False

        self.logger.debug("{:s} exists.".format(pidfile_path))

        with open(pidfile_path, "r") as f:
            try:
                p = int(f.read())
            except (OSError, ValueError) as exc:
                self.logger.error("No reabible PID within.")
                if raise_exc:
                    raise pid.PidFileUnreadableError(exc)
                else:
                    return False

        self.logger.debug("Read PID '{:d}' from file.".format(p))

        if not psutil.pid_exists(p):
            self.logger.debug("Process of PID '{:d}' not running.".format(p))
            return False

        self.logger.debug("Process of PID '{:d}' running.".format(p))

        try:
            cmd = psutil.Process(p).cmdline()
        except psutil.AccessDenied as exc:
            self.logger.error("No access to query PID '{:d}'."
                .format(pidfile_path, p))
            if raise_exc:
                raise exc
            else:
                return False

        self.logger.debug("PID '{:d}' command line '{}'".format(p,' '.join(cmd)))

        if cmd != self.command_line:
            self.logger.warn(
                "Running process command line does not agree with current '{}'"
                    .format(' '.join(self.command_line)))

        if raise_exc:
            raise pid.PidFileAlreadyRunningError(
                "Program already running with PID '{:d}'"
                    .format(p))
        else:
            return True


    def spawn_daemon(self):
        self.logger.info("Redirecting stdout to '{stdout:s}'"
            .format(stdout=self.outfile_name))
        self.logger.info("Redirecting stderr to '{stderr:s}'"
            .format(stderr=self.errfile_name))
        self.logger.info("Using PID file '{}'.".format(self.pidfile_name))

        # returns PID_CHECK_EMPTY, PID_CHECK_SAMEPID, PID_CHECK_NOTRUNNING, PID_CHECK_NOFILE
        # raises PidFileAlreadyRunningError, PidFileUnreadableError
        # only interested in exceptions, do not catch deliberately
        # pidstat = self.pidfile.check()
        # with pidfile.PIDFile(self.pidfile_name) as pid:
        #    if pid.is_running:
        #        self.logger.info("Apparently already running.")
        #        raise pidfile.AlreadyRunningError()
        self.check_daemon()

        d = daemon.DaemonContext(
            pidfile=pid.PidFile(
                pidname=self.pidfile_name,
                piddir=self.piddir
            ),
            stdout=self.outfile,
            stderr=self.errfile,
            signal_map = {  # treat a few common signals
                # signal.SIGTERM: self.shutdown, # treated in PidFile
                signal.SIGINT:  self.shutdown,
                signal.SIGHUP:  self.shutdown,
            })
        try:
            d.open()
        except pid.PidFileError as e:  #pid.base.PidFileAlreadyLockedError as e:
            self.logger.error(e)
            raise e

        self.logger.debug("Entered daemon context...")
        self.logger.debug("Created PID file '{}' for PID '{}'.".format(
            d.pidfile.filename, d.pidfile.pid))
        self.logger.debug("Working directory '{}'.".format(
            d.working_directory))

        self.spawn()

# derivatives
class DummyManager(FireWorksRocketLauncherManager):
    # generate descriptive pid files:

    @property
    def pidfile_name(self):
        return ".dummy.{user:s}@{host:}.pid".format(
            user=getpass.getuser(), host=socket.gethostname())

    @property
    def outfile_name(self):
        return os.path.join(self.logdir_loc,"dummy_{:s}.out"
            .format(self.timestamp))

    @property
    def errfile_name(self):
        return os.path.join(self.logdir_loc,"dummy_{:s}.err"
            .format(self.timestamp))

    def spawn(self):
        """Simple system shell dummy while loop for testing purposes"""
        args = ['while [ True ]; do printf "."; sleep 5; done']
        self.logger.info("Evoking '{cmd:s}'".format(cmd=' '.join(args)))
        p = subprocess.Popen(args,
            cwd = self.launchpad_loc,
            shell = True,
        )
        outs, errs = p.communicate()
        self.logger.info("Subprocess exited with return code = {}"
             .format(p.returncode))

# stubs
class RLaunchManager(FireWorksRocketLauncherManager):
    @property
    def pidfile_name(self):
        return ".rlaunch.{user:s}@{host:}.pid".format(
            user=getpass.getuser(), host=socket.gethostname())

    @property
    def outfile_name(self):
        return os.path.join(self.logdir_loc,"rlaunch_{:s}.out"
            .format(self.timestamp))

    @property
    def errfile_name(self):
        return os.path.join(self.logdir_loc,"rlaunch_{:s}.err"
            .format(self.timestamp))

    def spawn(self):
        """spawn rlaunch"""
        args = [ 'rlaunch',
                 '-l', self.fw_auth_file_path,
                 '-w', self.rlaunch_worker_file,
                 '--loglvl', 'DEBUG', 'rapidfire',
                 '--nlaunches', 'infinite',
                 '--sleep', self.rlaunch_interval,
               ]
        self.logger.info("Evoking '{cmd:s}'".format(cmd=' '.join(args)))
        p = subprocess.Popen(args, cwd = self.launchpad_loc)
        outs, errs = p.communicate()
        self.logger.info("Subprocess exited with return code = {}"
             .format(p.returncode))

class QLaunchManager(FireWorksRocketLauncherManager):
    @property
    def pidfile_name(self):
        return ".qlaunch.{user:s}@{host:}.pid".format(
            user=getpass.getuser(), host=socket.gethostname())

class RecoverOfflineManager(FireWorksRocketLauncherManager):
    @property
    def pidfile_name(self):
        return ".recover.{user:s}@{host:}.pid".format(
            user=getpass.getuser(), host=socket.gethostname())


class SSHTunnelManager(FireWorksRocketLauncherManager):
    @property
    def pidfile_name(self):
        return ".ssh_tunnel.{local_port:d}:{remote_user:s}@{remote_host:s}:{remote_port:d}.{user:s}@{host:}.pid".format(
            user=getpass.getuser(), host=socket.gethostname())


# CLI commands pendants

def start_dummy(args=None):
    fwrlm = DummyManager()
    fwrlm.spawn_daemon()

def start_rlaunch(args=None):
    fwrlm = RLaunchManager()
    fwrlm.spawn_daemon()


def main():
    import argparse

    # in order to have both:
    # * preformatted help text and ...
    # * automatic display of defaults
    class ArgumentDefaultsAndRawDescriptionHelpFormatter(
        argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
      pass

    parser = argparse.ArgumentParser(description=__doc__,
        formatter_class = ArgumentDefaultsAndRawDescriptionHelpFormatter)

    # root-level options
    parser.add_argument('--debug', default=False, required=False,
                           action='store_true', dest="debug", help='debug flag')
    parser.add_argument('--verbose', default=False, required=False,
                           action='store_true', dest="verbose", help='verbose flag')

    parser.add_argument('--log', required=False, nargs='?', dest="log",
                        default=None, const='fwrlm.log', metavar='LOG',
                        help='Write log file fwrlm.log, optionally specify log file name')

    # sub-commands
    subparsers = parser.add_subparsers(help='command', dest='command')

    # start command
    start_parser = subparsers.add_parser(
        'start', help='Start daemons.',
        formatter_class=ArgumentDefaultsAndRawDescriptionHelpFormatter)

    start_subparsers = start_parser.add_subparsers(
        help='subcommand', dest='subcommand')

    # start rlaunch
    start_rlaunch_parser = start_subparsers.add_parser(
        'rlaunch', help='Start rlaunch daemon')

    start_rlaunch_parser.set_defaults(func=start_rlaunch)

    # start dummy
    start_dummy_parser = start_subparsers.add_parser(
        'dummy', help='Start dummy daemon')

    start_dummy_parser.set_defaults(func=start_dummy)

    # stop command
    stop_parser = subparsers.add_parser(
        'stop', help='Stop daemons.',
        formatter_class=ArgumentDefaultsAndRawDescriptionHelpFormatter)

    stop_subparsers = stop_parser.add_subparsers(
      help='subcommand', dest='subcommand')

    args = parser.parse_args()

    # logging
    if args.debug:
        loglevel = logging.DEBUG
    elif args.verbose:
        loglevel = logging.INFO
    else:
        loglevel = logging.WARNING

    # logformat  = ''.join(("%(asctime)s",
    #  "[ %(filename)s:%(lineno)s - %(funcName)s() ]: %(message)s"))
    logformat  = "[ %(filename)s:%(lineno)s - %(funcName)s() ]: %(message)s"

    logging.basicConfig(level=loglevel, format=logformat)

    # explicitly modify the root logger (necessary?)
    logger = logging.getLogger()
    logger.setLevel(loglevel)

    # remove all handlers
    for h in logger.handlers: logger.removeHandler(h)

    # create and append custom handles
    ch = logging.StreamHandler()
    formatter = logging.Formatter(logformat)
    ch.setFormatter(formatter)
    ch.setLevel(loglevel)
    logger.addHandler(ch)

    if args.log:
        fh = logging.FileHandler(args.log)
        fh.setFormatter(formatter)
        fh.setLevel(loglevel)
        logger.addHandler(fh)

    if args.command is None:
        # if no command supplied, print help
        parser.print_help()
    elif 'func' not in args:
        parser.print_help()
    else:
        args.func(args)


if __name__ == '__main__':
    main()
