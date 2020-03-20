#!/usr/bin/env python3
"""Manages FireWorks rocket launchers and associated scripts as daemons"""

import os
import sys  # for stdout and stderr
import datetime  # for generating timestamps
import logging
#import pid  # for pidfiles, tested for v3.0.0

from imteksimfw.fireworks.utilities.fwrlm_base import pid
from imteksimfw.fireworks.utilities.fwrlm import DummyManager, \
    RLaunchManager, QLaunchManager, LPadRecoverOfflineManager, SSHTunnelManager

daemon_dict = {
    'dummy': DummyManager,
    'ssh': SSHTunnelManager,
    'rlaunch': RLaunchManager,
    'qlaunch': QLaunchManager,
    'recover': LPadRecoverOfflineManager,
}

# CLI commands pendants
def test_daemon(args):
    fwrlm = daemon_dict[ args.daemon ]()
    fwrlm.spawn()

def start_daemon(args):
    """Starts daemon and exits.

    Returns:
        int, sys.exit exit code
        -   0: daemon started
        - > 0: failure
    """
    logger = logging.getLogger(__name__)

    fwrlm = daemon_dict[ args.daemon ]()
    try:
        fwrlm.spawn_daemon()
        sys.exit(0)
    except Exception as exc:
        logger.exception(exc)
        sys.exit(1)

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
    logger.debug("{:s} daemon state: '{}'".format(args.daemon, stat))
    if stat == pid.PID_CHECK_RUNNING:
        logger.info("{:s} running.".format(args.daemon))
        ret = 0  # success, daemon running
    elif stat in [pid.PID_CHECK_NOFILE, pid.PID_CHECK_NOTRUNNING]:
        logger.info("{:s} not running.".format(args.daemon))
        ret = 3  # failure, daemon not running
    else:  # pid.PID_CHECK_UNREADABLE or pid.PID_CHECK_ACCESSDENIED
        logger.warn("{:s} state unknown.".format(args.daemon))
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

    # test command
    test_parser = subparsers.add_parser(
        'test', help='Runs service directly without detaching.',
        formatter_class=ArgumentDefaultsAndRawDescriptionHelpFormatter)

    test_parser.add_argument('daemon', type=str,
          help='Daemon name', metavar='DAEMON',
          choices=set(daemon_dict.keys()))

    test_parser.set_defaults(func=test_daemon)

    # parse
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
