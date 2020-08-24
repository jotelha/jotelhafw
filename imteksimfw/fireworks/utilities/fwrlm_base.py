#!/usr/bin/env python
#
# fwrlm_base.py
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
"""Manages FireWorks rocket launchers and associated scripts as daemons."""

import datetime  # for generating timestamps
import logging
import os
import signal  # for unix system signal treatment, see
# https://people.cs.pitt.edu/~alanjawi/cs449/code/shell/UnixSignals.htm
import sys  # for stdout and stderr

import daemon  # for detached daemons, tested for v2.2.4
import pid  # for pidfiles, tested for v3.0.0
import psutil  # checking process status

from imteksimfw.fireworks.fwrlm_config import \
  FW_CONFIG_PREFIX, FW_CONFIG_FILE_NAME, FW_AUTH_FILE_NAME, \
  DEFAULT_LOGLEVEL, \
  LAUNCHPAD_LOC, LOGDIR_LOC, MACHINE, SCHEDULER, \
  MONGODB_HOST, SSH_PORT_REMOTE, SSH_PORT_LOCAL, \
  FIREWORKS_DB, FIREWORKS_USER, FIREWORKS_PWD, \
  SSH_HOST, SSH_USER, SSH_KEY, \
  RLAUNCH_MULTI_NPROCESSES, \
  RLAUNCH_FWORKER_FILE, QLAUNCH_FWORKER_FILE, QADAPTER_FILE, \
  WEBGUI_USERNAME, WEBGUI_PASSWORD, WEBGUI_PORT

# define custom error codes
pid.PID_CHECK_UNREADABLE = "PID_CHECK_UNREADABLE"
pid.PID_CHECK_ACCESSDENIED = "PID_CHECK_ACCESSDENIED"
pid.PID_CHECK_RUNNING = "PID_CHECK_RUNNING"


class FireWorksRocketLauncherManager():
    """Base class for managing FireWorks-related daemons."""

    # The whole purpose of the following list of properties is to make
    # derivatives agnostic of the actual config mechanism in fwrlm_config
    # (reading key-value pairs from file, setting globals, importing here).
    # Someone might want to change that one day.

    # general properties
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
    def loglevel(self):
        return DEFAULT_LOGLEVEL

    @property
    def machine(self):
        return MACHINE

    @property
    def scheduler(self):
        return SCHEDULER

    # rlaunch related properties
    @property
    def rlaunch_fworker_file(self):
        return RLAUNCH_FWORKER_FILE if RLAUNCH_FWORKER_FILE else os.path.join(
            FW_CONFIG_PREFIX, "{:s}_noqueue_worker.yaml"
                .format(self.machine.lower()))

    @property
    def rlaunch_interval(self):
        return 10  # seconds

    # rlaunch multi rleated properties
    @property
    def rlaunch_multi_nprocesses(self):
        return RLAUNCH_MULTI_NPROCESSES

    # qlaunch related properties
    @property
    def qlaunch_fworker_file(self):
        return QLAUNCH_FWORKER_FILE if QLAUNCH_FWORKER_FILE else os.path.join(
            FW_CONFIG_PREFIX, "{:s}_queue_worker.yaml"
                .format(self.machine.lower()))

    @property
    def qadapter_file(self):
        return QADAPTER_FILE if QADAPTER_FILE else os.path.join(
            FW_CONFIG_PREFIX, "{:s}_{:s}_qadapter.yaml"
                .format(self.machine.lower(), self.scheduler.lower()))

    @property
    def qlaunch_interval(self):
        return 10  # seconds

    # lpad recover offline related properties
    @property
    def lpad_recover_offline_interval(self):
        return 10  # seconds

    # webgui related properties
    @property
    def webgui_port(self):
        return WEBGUI_PORT

    @property
    def webgui_username(self):
        return WEBGUI_USERNAME

    @property
    def webgui_password(self):
        return WEBGUI_PASSWORD

    # ssh related properties

    # only jump user implemented
    @property
    def jump_user(self):
        return SSH_USER

    @property
    def jump_host(self):
        return SSH_HOST

    @property
    def remote_host(self):
        return MONGODB_HOST

    @property
    def local_port(self):
        return SSH_PORT_LOCAL

    @property
    def remote_port(self):
        return SSH_PORT_REMOTE

    # only one ssh key implemented
    @property
    def ssh_key(self):
        return SSH_KEY

    @property
    def ssh_port(self):
        return 22

    @property
    def timestamp(self):
        return self._launchtime.strftime('%Y%m%d%H%M%S%f')

    # daemon administration related properties
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

        # no signal received yet, create list
        if not hasattr(self, '_received_signals'):
            self._received_signals = []

        # don't do anything if this signal is being handled already
        if signum not in self._received_signals:
            self._received_signals.append(signum)
            self.logger.debug("Recieved signal {}, shutting down..."
                .format(signum))

            # try to end all child processes
            os.killpg(os.getpgrp(), signal.SIGTERM)
            sys.exit(0)

    def get_pid(self):
        pidfile_path = os.path.join(self.piddir, self.pidfile_name)
        if not os.path.exists(pidfile_path):
            raise pid.PidFileUnreadableError(
                "{} does not exist.".format(pidfile_path))

        with open(pidfile_path, "r") as f:
            try:
                p = int(f.read())
            except (OSError, ValueError) as exc:
                raise pid.PidFileUnreadableError(exc)

        return p

    # inspired by
    # https://github.com/mosquito/python-pidfile/blob/master/pidfile/pidfile.py#L15
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
        pidfile_path = os.path.join(self.piddir, self.pidfile_name)
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

        self.logger.debug("PID '{:d}' command line '{}'"
            .format(p, ' '.join(cmd)))

        # this warning is somewhat obsolete
        if cmd != self.command_line:
            self.logger.debug("PID file process command line")
            self.logger.debug("")
            self.logger.debug("    {:s}".format(' '.join(cmd)))
            self.logger.debug("")
            self.logger.debug(
                "  does not agree with current process command line")
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
            detach_process=True,
            signal_map={  # treat a few common signals
                signal.SIGTERM: self.shutdown,  # otherwise treated in PidFile
                signal.SIGINT:  self.shutdown,
                signal.SIGHUP:  self.shutdown,
            })
        try:
            d.open()
        except pid.PidFileError as e:
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
