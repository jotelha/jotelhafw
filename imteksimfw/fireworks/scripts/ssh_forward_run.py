#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
#
# ssh_forward_run.py
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
"""Establishes an ssh forward via jump host through (free) local port."""

import logging
logger = logging.getLogger(__name__)
logfmt = "[%(levelname)s - %(filename)s:%(lineno)s - %(funcName)s() ] %(message)s (%(asctime)s)"
logging.basicConfig( format = logfmt )

from imteksimfw.fireworks.utilities.ssh_forward import forward

def main():
    import argparse

    parser = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--remote-host',
        help="Remote server",
        default='ufr2.isi1.public.ads.uni-freiburg.de')
    parser.add_argument('--remote-port',
        help="Remote port",
        type=int,
        default=445)
    parser.add_argument('--local-port',
        help="Local port, picked randomly if not specified",
        default=None)
    parser.add_argument('--ssh-user',
        help="User name on SSH jump host",
        default='sshclient')
    parser.add_argument('--ssh-host',
        help="SSH jump host",
        default='132.230.102.164')
    parser.add_argument('--ssh-keyfile',
        help='SSH key file (no password login supported)',
        default='~/.ssh/sshclient-frrzvm')
    parser.add_argument('--ssh-port',
        help="SSH port",
        default=22)
    parser.add_argument('--port-file',
        help='File to hold (possibly randomly chosen) port number if specified',
        default=None)
    parser.add_argument('--verbose', '-v', action='store_true',
        help='Make this tool more verbose')
    parser.add_argument('--debug', action='store_true',
        help='Make this tool print debug info')

    try:
        import argcomplete
        argcomplete.autocomplete(parser)
    except:
        pass

    args = parser.parse_args()

    if args.debug:
        loglevel = logging.DEBUG
    elif args.verbose:
        loglevel = logging.INFO
    else:
        loglevel = logging.WARNING

    logger.setLevel(loglevel)

    logger.debug( args )

    logger.info("Forwarding localhost:{} to {:s}:{:d} via ssh:{:s}@{:s}:{:d}".format(
        args.local_port,
        args.remote_host,
        args.remote_port,
        args.ssh_user,
        args.ssh_host,
        args.ssh_port  ) )

    forward(
        remote_host = args.remote_host,
        remote_port = args.remote_port,
        local_port  = args.local_port,
        ssh_host    = args.ssh_host,
        ssh_user    = args.ssh_user,
        ssh_keyfile = args.ssh_keyfile,
        ssh_port    = args.ssh_port,
        port_file   = args.port_file )

if __name__ == '__main__':
    main()
