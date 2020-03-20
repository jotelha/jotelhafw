#!/usr/bin/env python
"""
Establishes an ssh forward via jump host through (free) local port
"""

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
