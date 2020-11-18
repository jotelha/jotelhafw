# coding: utf-8
#
# ssh_tasks.py
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
__author__ = 'Johannes Laurin Hoermann'
__copyright__ = 'Copyright 2020, IMTEK Simulation, University of Freiburg'
__email__ = 'johannes.hoermann@imtek.uni-freiburg.de, johannes.laurin@gmail.com'
__date__ = 'May 7, 2020'

from contextlib import ExitStack

import io
import logging
import os.path
import select
import socket
import threading
import time

try:
    import SocketServer
except ImportError:
    import socketserver as SocketServer
import paramiko

from fireworks.fw_config import FW_LOGGING_FORMAT
from fireworks.core.firework import FWAction
from fireworks.utilities.dict_mods import get_nested_dict_value
from fireworks.utilities.fw_serializers import ENCODING_PARAMS

from imteksimfw.utils.multiprocessing import RunAsChildProcessTask
from imteksimfw.utils.logging import LoggingContext

DEFAULT_FORMATTER = logging.Formatter(FW_LOGGING_FORMAT)


def from_fw_spec(param, fw_spec):
    """Expands param['key'] as key within fw_spec.

    If param is dict hand has field 'key', then return value at specified
    position from fw_spec. Otherwise, return 'param' itself.
    """
    if isinstance(param, dict) and 'key' in param:
        ret = get_nested_dict_value(fw_spec, param['key'])
    else:
        ret = param
    return ret


class ForwardServer(SocketServer.ThreadingTCPServer):
    """
    This class has been copied and modified from the original paramiko
    repository at
    https://github.com/paramiko/paramiko/blob/master/demos/forward.py
    and underlies the GNU Lesser General Public Licesnse v 2.1 at
    https://github.com/paramiko/paramiko/blob/master/LICENSE.
    """
    daemon_threads = True
    allow_reuse_address = True


class Handler(SocketServer.BaseRequestHandler):
    """
    This class has been copied and modified from the original paramiko
    repository at
    https://github.com/paramiko/paramiko/blob/master/demos/forward.py
    and underlies the GNU Lesser General Public Licesnse v 2.1 at
    https://github.com/paramiko/paramiko/blob/master/LICENSE.
    """
    def handle(self):
        logger = logging.getLogger(__name__)
        try:
            chan = self.ssh_transport.open_channel(
                "direct-tcpip",
                (self.chain_host, self.chain_port),
                self.request.getpeername(),
            )
        except Exception as e:
            logger.info(
                "Incoming request to %s:%d failed: %s"
                % (self.chain_host, self.chain_port, repr(e))
            )
            return
        if chan is None:
            logger.info(
                "Incoming request to %s:%d was rejected by the SSH server."
                % (self.chain_host, self.chain_port)
            )
            return

        logger.info(
            "Connected!  Tunnel open %r -> %r -> %r"
            % (
                self.request.getpeername(),
                chan.getpeername(),
                (self.chain_host, self.chain_port),
            )
        )
        while True:
            r, w, x = select.select([self.request, chan], [], [])
            if self.request in r:
                data = self.request.recv(1024)
                if len(data) == 0:
                    break
                chan.send(data)
            if chan in r:
                data = chan.recv(1024)
                if len(data) == 0:
                    break
                self.request.send(data)

        peername = self.request.getpeername()
        chan.close()
        self.request.close()
        logger.info("Tunnel closed from %r" % (peername,))


class SSHForwardTask(RunAsChildProcessTask):
    """
    A Firetask to open ssh port forwarding via jump host.

    Required params:
        - remote_host: target remote host
        - remote_port: target port at remote host
        - ssh_host: intermediate ssh jump host
        - ssh_user: ssh user at intermediate jump host
    Optional params:
        - local_port (int, default: None): local port, allocate random free
            port if not specified.
        - port_file (str, default: '.port'): write allocated local
            port number to that file.
        - ssh_port (int, default: 22): ssh port at intermediate jump host
        - ssh_keyfile (st, default '~/.ssh/id_rsa'): ssh identity file to
            establish connection to intermediate jump host

        All of the above required and optional parameters may also be dicts of
        format { 'key': 'some->nested->fw_spec->key' } for looking up values
        within fw_spec instead.

        - store_stdlog (bool, default: False): insert log output into database
        - stdlog_file (str, Default: NameOfTaskClass.log): print log to file
        - loglevel (str, Default: logging.INFO): loglevel for this task

    Example:

        t = SSHForwardTask(
            remote_host="ufr2.isi1.public.ads.uni-freiburg.de",
            remote_port=445,
            local_port=10445,
            ssh_host="simdata.vm.uni-freiburg.de",
            ssh_user="sshclient",
            ssh_keyfile="~/.ssh/sshclient-frrzvm",
            ssh_port=22,
            port_file='.port')

        establishes the Paramiko equivalent of the command line ssh call

            ssh  -i ~/.ssh/sshclient-frrzvm -N -L \
              10445:ufr2.isi1.public.ads.uni-freiburg.de:445 \
              sshclient@132.230.102.164

        Set 'local_port' to None to allocate a random free port. Read that
        port from 'port_file' afte connection has been established.
    """
    required_params = [
        *RunAsChildProcessTask.required_params,
        'remote_host',
        'remote_port',
        'ssh_host',
        'ssh_user',
    ]

    optional_params = [
        *RunAsChildProcessTask.optional_params,
        'local_port',
        'port_file',
        'ssh_port',
        'ssh_keyfile',

        'store_stdlog',
        'stdlog_file',
        'loglevel',
    ]

    def _run_task_as_child_process(self, fw_spec, q, e=None):
        """q is a Queue used to return fw_action, e is an Event to end task."""
        # required
        remote_host = from_fw_spec(self.get('remote_host'), fw_spec)
        remote_port = from_fw_spec(self.get('remote_port'), fw_spec)
        ssh_host = from_fw_spec(self.get('ssh_host'), fw_spec)
        ssh_user = from_fw_spec(self.get('ssh_user'), fw_spec)

        # optional with defaults
        local_port = from_fw_spec(self.get("local_port", None), fw_spec)
        port_file = from_fw_spec(self.get("port_file", '.port'), fw_spec)

        ssh_port = from_fw_spec(self.get("ssh_port", 22), fw_spec)
        ssh_port = int(ssh_port)
        assert isinstance(ssh_port, int)

        ssh_keyfile = from_fw_spec(
            self.get("ssh_keyfile", '~/.ssh/id_rsa'), fw_spec)

        stdlog_file = self.get('stdlog_file', '{}.log'.format(self._fw_name))
        store_stdlog = self.get('store_stdlog', False)

        loglevel = self.get('loglevel', logging.INFO)

        with ExitStack() as stack:

            if store_stdlog:
                stdlog_stream = io.StringIO()
                logh = logging.StreamHandler(stdlog_stream)
                logh.setFormatter(DEFAULT_FORMATTER)
                stack.enter_context(
                    LoggingContext(handler=logh, level=loglevel, close=False))

            # logging to dedicated log file if desired
            if stdlog_file:
                logfh = logging.FileHandler(stdlog_file, mode='a', **ENCODING_PARAMS)
                logfh.setFormatter(DEFAULT_FORMATTER)
                stack.enter_context(
                    LoggingContext(handler=logfh, level=loglevel, close=True))

            logger = logging.getLogger(__name__)

            # allocate free local port if none specified
            # inspired by
            #   https://www.scivision.dev/python-get-free-network-port
            # and
            #   http://code.activestate.com/recipes/531822-pick-unused-port
            if local_port is None:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.bind(('localhost', 0))
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                addr, local_port = s.getsockname()
                logger.info(
                    "Allocated free local port: {:d}".format(local_port))

            # write port number to file for communication with other processes
            if isinstance(port_file, str):
                with open(port_file, mode='w') as f:
                    f.write(str(local_port))
                logger.info("Wrote port number to '{}'".format(port_file))

            transport = paramiko.Transport((ssh_host, ssh_port))
            pkey = paramiko.RSAKey.from_private_key_file(
                os.path.expanduser(ssh_keyfile))

            transport.connect(hostkey=None,
                              username=ssh_user,
                              password=None,
                              pkey=pkey)

            class SubHander(Handler):
                chain_host = remote_host
                chain_port = remote_port
                ssh_transport = transport

            with ForwardServer(("", local_port), SubHander) as server:
                # if a stop signal is to be expected, then run server in
                # separate thread and forward stop signal
                if e is not None:
                    server_thread = threading.Thread(
                        target=server.serve_forever)
                    # Exit the server thread when the main thread terminates
                    server_thread.daemon = True
                    server_thread.start()
                    logger.info(
                        "Server loop running in thread '{}'".format(
                            server_thread.name))

                    while server_thread.is_alive():
                        if e.is_set():
                            logger.info("Send stop signal to server loop.")
                            server.shutdown()
                        time.sleep(1)
                else:
                    server.serve_forever()

                logger.info("Server stoped.")

            try:
                logger.info("Trying to close socket.")
                s.close()  # fails if s does not exist
            except Exception:
                logger.info("Apparently already closed.")

        q.put(FWAction())
