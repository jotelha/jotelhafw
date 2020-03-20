#!/usr/bin/env python3
"""Manages FireWorks rocket launchers and associated scripts as daemons"""

import os
import signal  # for unix system signal treatment (https://people.cs.pitt.edu/~alanjawi/cs449/code/shell/UnixSignals.htm)
import sys  # for stdout and stderr
import daemon  # for detached daemons, tested for v2.2.4
import datetime  # for generating timestamps
import getpass  # get username
import logging
import pid  # for pidfiles, tested for v3.0.0
import psutil  # checking process status
import socket  # for host name
import subprocess

from imteksimfw.fwrlm_config import \
  FW_CONFIG_PREFIX, FW_CONFIG_FILE_NAME, FW_AUTH_FILE_NAME, \
  LAUNCHPAD_LOC, LOGDIR_LOC, MACHINE, SCHEDULER, \
  MONGODB_HOST, MONGODB_PORT_REMOTE, MONGODB_PORT_LOCAL, \
  FIREWORKS_DB, FIREWORKS_USER, FIREWORKS_PWD, \
  SSH_HOST, SSH_USER, SSH_TUNNEL, SSH_KEY, USE_RSTUNNEL, RSTUNNEL_CONFIG, \
  RECOVER_OFFLINE, RLAUNCH_FWORKER_FILE, QLAUNCH_FWORKER_FILE, QADAPTER_FILE, \
  MULTI_RLAUNCH_NTASKS,  OMP_NUM_THREADS, FWGUI_PORT


# define custom error codes
pid.PID_CHECK_UNREADABLE = "PID_CHECK_UNREADABLE"
pid.PID_CHECK_ACCESSDENIED = "PID_CHECK_ACCESSDENIED"
pid.PID_CHECK_RUNNING = "PID_CHECK_RUNNING"

from imteksimfw.fwrlm_config import config_to_dict, config_keys_to_list

class FireWorksRocketLauncherManager():
    """Base class for managing FireWorks-related daemons"""
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
    def timestamp(self):
        return self._launchtime.strftime('%Y%m%d%H%M%S%f')

    # daemon-administration related
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
        self._launchtime = datetime.datetime.now()
        self.logger.debug("Launched at {:s} as".format(
            self._launchtime.strftime('%Y-%m-%d %H:%M:%S.%f')))
        self.logger.debug("")
        self.logger.debug("    {:s}".format(' '.join(self.command_line)))
        self.logger.debug("")

    def shutdown(self, signum, frame):
        """Shut down daemon neatly"""
        # try to end all child processes
        self.logger.debug("Recieved signal {}, shutting down..."
            .format(signum))
        os.killpg(os.getpgrp(), signal.SIGTERM)
        sys.exit(0)

    def get_pid(self):
        pidfile_path = os.path.join(self.piddir,self.pidfile_name)
        if not os.path.exists(pidfile_path):
            raise PidFileUnreadableError(
                "{} does not exist.".format(pidfile_path))

        with open(pidfile_path, "r") as f:
            try:
                p = int(f.read())
            except (OSError, ValueError) as exc:
                raise pid.PidFileUnreadableError(exc)

        return p

    # inspired by https://github.com/mosquito/python-pidfile/blob/d2ac227a5c3635db2541aa8d8e4507bdeb4b34e2/pidfile/pidfile.py#L15
    def check_daemon(self, raise_exc=True):
        """Check for a running process by PID file

        Args:
            raise_exc (bool): If true, only returns successfully either with
                              `PID_CHECK_NOFILE` or `PID_CHECK_NOTRUNNING` if
                              no running process was found, otherwise raises 
                              exceptions (see below). If false, always returns
                              with more specific code (default: True).
        Returns:

            str, in case of success:
            - pid.PID_CHECK_NOFILE       No PID file found.
            - pid.PID_CHECK_NOTRUNNING   PID file found and evaluated,
                                         but no such process running.

            str, in case of failure and `raise_exc = False`
            - pid.PID_CHECK_UNREADABLE   PID file found, but no PID
                                         could be read from it.
            - pid.PID_CHECK_RUNNING      PID file found and evaluated,
                                         process running.
            - pid.PID_CHECK_ACCESSDENIED PID file found and evaluated,
                                         process running, but we do not
                                         have rights to query it.

        Raises (in case of `raise_exc = True`,
            corresponding to the three failure return codes above):

            pid.PidFileUnreadableError
            pid.PidFileAlreadyRunningError
            psutil.AccessDenied

        """
        pidfile_path = os.path.join(self.piddir,self.pidfile_name)
        if not os.path.exists(pidfile_path):
            self.logger.debug("{} does not exist.".format(pidfile_path))
            return pid.PID_CHECK_NOFILE

        self.logger.debug("{:s} exists.".format(pidfile_path))

        with open(pidfile_path, "r") as f:
            try:
                p = int(f.read())
            except (OSError, ValueError) as exc:
                self.logger.error("No reabible PID within.")
                if raise_exc:
                    raise pid.PidFileUnreadableError(exc)
                else:
                    return pid.PID_CHECK_UNREADABLE

        self.logger.debug("Read PID '{:d}' from file.".format(p))

        if not psutil.pid_exists(p):
            self.logger.debug("Process of PID '{:d}' not running.".format(p))
            return pid.PID_CHECK_NOTRUNNING

        self.logger.debug("Process of PID '{:d}' running.".format(p))

        try:
            cmd = psutil.Process(p).cmdline()
        except psutil.AccessDenied as exc:
            self.logger.error("No access to query PID '{:d}'."
                .format(pidfile_path, p))
            if raise_exc:
                raise exc
            else:
                return pid.PID_CHECK_ACCESSDENIED

        self.logger.debug("PID '{:d}' command line '{}'".format(p,' '.join(cmd)))

        if cmd != self.command_line:
            self.logger.debug("PID file process command line")
            self.logger.debug("")
            self.logger.debug("    {:s}".format(' '.join(cmd)))
            self.logger.debug("")
            self.logger.debug("  does not agree with current process command line")
            self.logger.debug("")
            self.logger.debug("    {:s}".format(' '.join(self.command_line)))
            self.logger.debug("")

        if raise_exc:
            raise pid.PidFileAlreadyRunningError(
                "Program already running with PID '{:d}'"
                    .format(p))
        else:
            return pid.PID_CHECK_RUNNING

    def spawn_daemon(self):
        self.logger.debug("Redirecting stdout to '{stdout:s}'"
            .format(stdout=self.outfile_name))
        self.logger.debug("Redirecting stderr to '{stderr:s}'"
            .format(stderr=self.errfile_name))
        self.logger.debug("Using PID file '{}'.".format(self.pidfile_name))

        # only interested in exceptions, do not catch deliberately
        pidstat = self.check_daemon()
        self.logger.debug("PID file state: {}.".format(pidstat))

        self.pidfile = pid.PidFile(
                pidname=self.pidfile_name,
                piddir=self.piddir
            )

        d = daemon.DaemonContext(
            pidfile=self.pidfile,
            stdout=self.outfile,
            stderr=self.errfile,
            signal_map = {  # treat a few common signals
                signal.SIGTERM: self.shutdown,  # otherwise treated in PidFile
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

    def stop_daemon(self):
        """Exit the daemon process specified in the current PID file.

        Returns:
            bool: True for successfully stopped, False if not running.

        Raises:
            daemon.DaeminRunnerStopFailureError
        """
        try:
            pidstat = self.check_daemon()
        except pid.PidFileAlreadyRunningError:
            pass  # It's running and we will stop it.
        else:  # No exception means not running, nothing to stop.
            self.logger.debug("Daemon not running ({}, nothing to do..."
                .format(pidstat))
            return False
        # We don't catch other unknown state exceptions...

        p = self.get_pid()
        # PID should be available here, no exceptions to be expected.

        try:
            os.killpg(os.getpgid(p), signal.SIGTERM)
        except OSError as exc:
            raise daemon.DaemonRunnerStopFailureError(
                "Failed to terminate {pid:d}: {exc}".format(
                    pid=p, exc=exc))

        return True

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
        self.logger.debug("Evoking '{cmd:s}'".format(cmd=' '.join(args)))
        p = subprocess.Popen(args,
            cwd = self.launchpad_loc,
            shell = True,
        )
        outs, errs = p.communicate()
        self.logger.debug("Subprocess exited with return code = {}"
             .format(p.returncode))

class SSHAgentManager(FireWorksRocketLauncherManager):
    @property
    def pidfile_name(self):
        return ".ssh_tunnel.{local_port:d}:{remote_user:s}@{remote_host:s}:{remote_port:d}.{user:s}@{host:}.pid".format(
            user=getpass.getuser(), host=socket.gethostname())

class SSHTunnelManager(FireWorksRocketLauncherManager):
    @property
    def pidfile_name(self):
        return ".ssh_tunnel.{local_port:d}:{remote_user:s}@{remote_host:s}:{remote_port:d}.{user:s}@{host:}.pid".format(
            user=getpass.getuser(), host=socket.gethostname())

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

    @property
    def rlaunch_fworker_file(self):
        return RLAUNCH_FWORKER_FILE if RLAUNCH_FWORKER_FILE else os.path.join(
            FW_CONFIG_PREFIX, "{:s}_noqueue_worker.yaml"
                .format(self.machine.lower()))

    @property
    def rlaunch_interval(self):
        return 10  # seconds


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

class RecoverOfflineManager(FireWorksRocketLauncherManager):
    @property
    def pidfile_name(self):
        return ".recover.{user:s}@{host:}.pid".format(
            user=getpass.getuser(), host=socket.gethostname())


daemon_dict = {
    'dummy': DummyManager,
    'rlaunch': RLaunchManager,
}

# CLI commands pendants

def start_daemon(args):
    fwrlm = daemon_dict[ args.daemon ]()
    fwrlm.spawn_daemon()

def check_daemon_status(args):
    """Checks status of daemon and exits.

    Returns:
        int, sys.exit exit code
        - 0: daemon running
        - 3: daemon not running
        - 4: state unknown

    Exit codes follow `systemctl`'s exit codes, see
    https://www.freedesktop.org/software/systemd/man/systemctl.html#Exit%20status
    """
    logger = logging.getLogger(__name__)

    fwrlm = daemon_dict[ args.daemon ]()
    stat = fwrlm.check_daemon(raise_exc=False)
    logger.info("{:s} daemon state: '{}'".format(args.daemon, stat))
    if stat == pid.PID_CHECK_RUNNING:
        ret = 0  # success, daemon running
    elif stat in [pid.PID_CHECK_NOFILE, pid.PID_CHECK_NOTRUNNING]:
        ret = 3  # failure, daemon not running
    else:  # pid.PID_CHECK_UNREADABLE or pid.PID_CHECK_ACCESSDENIED
        ret = 4  # failure, state unknown
    sys.exit(ret)

def stop_daemon(args):
    logger = logging.getLogger(__name__)

    fwrlm = daemon_dict[ args.daemon ]()
    try:
        stat = fwrlm.stop_daemon()
    except Exception as exc:  # stopping failed
        logger.exception(exc)
        sys.exit(4)

    if stat:  # successfully stopped
        logger.info("Stopped.")
    else:  # wasn't running anyway
        logger.info("No daemon running.")
    sys.exit(0)

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

    start_parser.add_argument('daemon', type=str,
          help='Daemon name', metavar='DAEMON',
          choices=set(daemon_dict.keys()))

    start_parser.set_defaults(func=start_daemon)

    # status command
    status_parser = subparsers.add_parser(
        'status', help='Query daemon status.',
        formatter_class=ArgumentDefaultsAndRawDescriptionHelpFormatter)

    status_parser.add_argument('daemon', type=str,
          help='Daemon name', metavar='DAEMON',
          choices=set(daemon_dict.keys()))

    status_parser.set_defaults(func=check_daemon_status)

    # stop command
    stop_parser = subparsers.add_parser(
        'stop', help='Stop daemons.',
        formatter_class=ArgumentDefaultsAndRawDescriptionHelpFormatter)

    stop_parser.add_argument('daemon', type=str,
          help='Daemon name', metavar='DAEMON',
          choices=set(daemon_dict.keys()))

    stop_parser.set_defaults(func=stop_daemon)

    args = parser.parse_args()

    # logging
    logformat  = "%(levelname)s: %(message)s"
    if args.debug:
        logformat  = "[%(asctime)s-%(funcName)s()-%(filename)s:%(lineno)s] %(levelname)s: %(message)s"
        loglevel = logging.DEBUG
    elif args.verbose:
        loglevel = logging.INFO
    else:
        loglevel = logging.WARNING

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
