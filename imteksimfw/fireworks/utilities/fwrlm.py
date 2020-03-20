#!/usr/bin/env python3
#
# fwrlm.py
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
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
"""Manages FireWorks rocket launchers and associated scripts as daemons"""

import os
import signal  # for unix system signal treatment, see
# https://people.cs.pitt.edu/~alanjawi/cs449/code/shell/UnixSignals.htm
import sys  # for stdout and stderr
import daemon  # for detached daemons, tested for v2.2.4
import datetime  # for generating timestamps
import getpass  # get username
import logging
import pid  # for pidfiles, tested for v3.0.0
import psutil  # checking process status
import socket  # for host name
import subprocess

from imteksimfw.fireworks.fwrlm_config import \
  FW_CONFIG_PREFIX, FW_CONFIG_FILE_NAME, FW_AUTH_FILE_NAME, \
  LAUNCHPAD_LOC, LOGDIR_LOC, MACHINE, SCHEDULER, \
  MONGODB_HOST, MONGODB_PORT_REMOTE, MONGODB_PORT_LOCAL, \
  FIREWORKS_DB, FIREWORKS_USER, FIREWORKS_PWD, \
  SSH_HOST, SSH_USER, SSH_TUNNEL, SSH_KEY, USE_RSTUNNEL, RSTUNNEL_CONFIG, \
  RECOVER_OFFLINE, RLAUNCH_FWORKER_FILE, QLAUNCH_FWORKER_FILE, QADAPTER_FILE, \
  MULTI_RLAUNCH_NTASKS,  OMP_NUM_THREADS, FWGUI_PORT


from imteksimfw.fireworks.utilities.fwrlm_base import FireWorksRocketLauncherManager

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


class SSHTunnelManager(FireWorksRocketLauncherManager):
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
        return MONGODB_PORT_LOCAL

    @property
    def remote_port(self):
        return MONGODB_PORT_REMOTE

    # only one ssh key implemented
    @property
    def ssh_key(self):
        return SSH_KEY

    @property
    def ssh_port(self):
        return 22

    @property
    def pidfile_name(self):
        return (
            ".ssh_tunnel.{local_port:d}:@{remote_host:s}:{remote_port:d}"
            ":{jump_user:s}@{jump_host:}.{local_user:s}@{local_host:s}.pid"
            ).format(
              local_port=self.local_port,
              remote_host=self.remote_host,
              remote_port=self.remote_port,
              jump_user=self.jump_user,
              jump_host=self.jump_host,
              local_user=getpass.getuser(),
              local_host=socket.gethostname())

    @property
    def outfile_name(self):
        return os.path.join(self.logdir_loc,"ssh_tunnel_{:s}.out"
            .format(self.timestamp))

    @property
    def errfile_name(self):
        return os.path.join(self.logdir_loc,"ssh_tunnel_{:s}.err"
            .format(self.timestamp))

    def spawn(self):
        """SSH forward based on FWRLM_config.yaml settings"""
        from imteksimfw.fireworks.utilities.ssh_forward import forward
        forward(
            remote_host = self.remote_host,
            remote_port = self.remote_port,
            local_port  = self.local_port,
            ssh_host    = self.jump_host,
            ssh_user    = self.jump_user,
            ssh_keyfile = self.ssh_key,
            ssh_port    = self.ssh_port,
            port_file   = None)


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
                 '-w', self.rlaunch_fworker_file,
                 '--loglvl', 'DEBUG', 'rapidfire',
                 '--nlaunches', 'infinite',
                 '--sleep', self.rlaunch_interval,
               ]
        args = [ a if isinstance(a, str) else str(a) for a in args ]
        self.logger.info("Evoking '{cmd:s}'".format(cmd=' '.join(args)))
        p = subprocess.Popen(args, cwd = self.launchpad_loc)
        outs, errs = p.communicate()
        self.logger.info("Subprocess exited with return code = {}"
             .format(p.returncode))

# stubs
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
