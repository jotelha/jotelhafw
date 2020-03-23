#!/usr/bin/env python
#
# fwrlm_run.py
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
"""Manages FireWorks rocket launchers and associated scripts as daemons"""

import argparse
import os
import shutil
import sys  # for stdout and stderr
import logging
import multiprocessing

from imteksimfw.fireworks.utilities.fwrlm_base import pid
from imteksimfw.fireworks.utilities.fwrlm import DummyManager, \
    RLaunchManager, QLaunchManager, LPadRecoverOfflineManager, \
    LPadWebGuiManager, SSHTunnelManager

from imteksimfw.fireworks.fwrlm_config import config_to_dict, \
    FW_CONFIG_PREFIX, FW_CONFIG_SKEL_PREFIX, FW_CONFIG_TEMPLATE_PREFIX

from imteksimfw.fireworks.utilities.render import render_batch

daemon_dict = {
    'dummy': DummyManager,
    'ssh': SSHTunnelManager,
    'rlaunch': RLaunchManager,
    'qlaunch': QLaunchManager,
    'recover': LPadRecoverOfflineManager,
    'webgui': LPadWebGuiManager,
}

daemon_sets = {
    'all': ['ssh', 'rlaunch', 'qlaunch', 'recover', 'webgui'],
    'remote_worker': ['ssh', 'rlaunch', 'qlaunch', 'recover'],
    'fw':  ['rlaunch', 'qlaunch', 'recover'],
    **{k: [k] for k in daemon_dict.keys()},
}

EX_OK = 0
EX_FAILURE = 1
EX_NOTRUNNING = 1
EX_UNKNOWN = 4


# CLI commands pendants
def test_daemon(daemon):
    """Run directly for testing purposes."""
    fwrlm = daemon_dict[daemon]()
    fwrlm.spawn()


def start_daemon(daemon):
    """Start daemon and exit.

    Returns:
        int, sys.exit exit code
        -   0: daemon started
        - > 0: failure
    """
    logger = logging.getLogger(__name__)

    fwrlm = daemon_dict[daemon]()
    try:
        p = multiprocessing.Process(target=fwrlm.spawn_daemon)
        p.start()
        p.join()
        ret = EX_OK
    except Exception as exc:
        logger.exception(exc)
        ret = EX_FAILURE
    else:
        logger.info("{:s} started.".format(daemon))
    return ret


def check_daemon_status(daemon):
    """Check status of daemon and exit.

    Returns:
        int, sys.exit exit code
        - 0: daemon running
        - 1: daemon not running
        - 4: state unknown

    Exit codes follow `systemctl`'s exit codes with the exception of
    1 for a non-running daemon instead of 3, see
    https://www.freedesktop.org/software/systemd/man/systemctl.html#Exit%20status
    """
    logger = logging.getLogger(__name__)

    fwrlm = daemon_dict[daemon]()
    stat = fwrlm.check_daemon(raise_exc=False)
    logger.debug("{:s} daemon state: '{}'".format(daemon, stat))
    if stat == pid.PID_CHECK_RUNNING:
        logger.info("{:s} running.".format(daemon))
        ret = EX_OK  # success, daemon running
    elif stat in [pid.PID_CHECK_NOFILE, pid.PID_CHECK_NOTRUNNING]:
        logger.info("{:s} not running.".format(daemon))
        ret = EX_NOTRUNNING  # failure, daemon not running
    else:  # pid.PID_CHECK_UNREADABLE or pid.PID_CHECK_ACCESSDENIED
        logger.warning("{:s} state unknown.".format(daemon))
        ret = EX_UNKNOWN  # failure, state unknown
    return ret


def stop_daemon(daemon):
    """Stop daemon and exit."""
    logger = logging.getLogger(__name__)

    fwrlm = daemon_dict[daemon]()
    try:
        stat = fwrlm.stop_daemon()
    except Exception as exc:  # stopping failed
        logger.exception(exc)
        ret = EX_UNKNOWN
    else:
        if stat:  # successfully stopped
            logger.info("{} stopped.".format(daemon))
        else:  # wasn't running anyway
            logger.info("{} not running.".format(daemon))
        ret = EX_OK
    return ret


def restart_daemon(daemon):
    """Restart daemon and exit."""
    import time
    ret = stop_daemon(daemon)
    if ret == os.EX_OK:
        time.sleep(5)
        ret = start_daemon(daemon)
    return ret


def act(args, action):
    """Perform any action (start, stop, ...) for one or multiple daemons."""
    logger = logging.getLogger(__name__)
    daemons = {d for s in args.daemon for d in daemon_sets[s]}
    logger.debug("Will evoke '{}' for set [{}]".format(
        action.__name__, ', '.join(list(daemons))))
    ret = EX_OK
    for daemon in daemons:
        logger.debug("Evoking '{}' for {}".format(action.__name__, daemon))
        cur_ret = action(daemon)
        logger.debug("'{}' for {} returned with exit code '{}'".format(
            action.__name__, daemon, cur_ret))
        ret = cur_ret if cur_ret > ret else ret
    sys.exit(ret)


# configuration administration
def reset_config(args):
    """Resets FireWorks user config."""
    logger = logging.getLogger(__name__)

    if os.path.exists(args.config_dir) and args.force:
        logger.warning("Directory '{:s}' exists and will be deleted."
                       .format(args.config_dir))
        shutil.rmtree(args.config_dir)
    elif os.path.exists(args.config_dir):
        msg = ( "Directory '{:s}' does exist! Use '--force' to remove."
                .format(args.config_dir) )
        logger.error(msg)
        raise FileExistsError(msg)

    logger.info("Copy skeleton from {:s} to {:s}.".format(
        args.skel_dir, args.config_dir))
    shutil.copytree(args.skel_dir, args.config_dir)

    context = config_to_dict()
    # only use key : value pairs with value not None
    args.context = {key: value for key, value in context.items() if value}

    logger.info("Render config tempaltes in {:s}".format(args.template_dir))
    logger.info("    to {:s}".format(args.config_dir))
    logger.debug("    with context {}".format(args.context))

    render_batch(
        args.template_dir,
        args.config_dir,
        args.context)


def show_config(args):
    """Show FWRLM config."""
    config_dict = config_to_dict()
    for key, value in config_dict.items():
        print("{:s}={:s}".format(str(key),str(value)))


def main():
    """FWRLM command line interface."""
    # multiprocessing.set_start_method('fork')

    # in order to have both:
    # * preformatted help text and ...
    # * automatic display of defaults
    class ArgumentDefaultsAndRawDescriptionHelpFormatter(
            argparse.ArgumentDefaultsHelpFormatter,
            argparse.RawDescriptionHelpFormatter):
        """Allows for both preformatted help and automatic defaults display."""
        pass

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=ArgumentDefaultsAndRawDescriptionHelpFormatter)

    # root-level options
    parser.add_argument('--debug', default=False, required=False,
                        action='store_true', dest="debug", help='debug')
    parser.add_argument('--verbose', default=False, required=False,
                        action='store_true', dest="verbose", help='verbose')

    parser.add_argument('--log', required=False, nargs='?', dest="log",
                        default=None, const='fwrlm.log', metavar='LOG',
                        help='Write log fwrlm.log, optionally specify log name')

    # sub-commands
    subparsers = parser.add_subparsers(help='command', dest='command')

    # start command
    start_parser = subparsers.add_parser(
        'start', help='Start daemons.',
        formatter_class=ArgumentDefaultsAndRawDescriptionHelpFormatter)

    start_parser.add_argument(
        'daemon', type=str, nargs='+',
        help='Daemon name', metavar='DAEMON',
        choices=set(daemon_sets.keys()))

    start_parser.set_defaults(func=lambda args: act(args, start_daemon))

    # status command
    status_parser = subparsers.add_parser(
        'status', help='Query daemon status.',
        formatter_class=ArgumentDefaultsAndRawDescriptionHelpFormatter)

    status_parser.add_argument(
        'daemon', type=str, nargs='+',
        help='Daemon name', metavar='DAEMON',
        choices=set(daemon_sets.keys()))

    status_parser.set_defaults(
        func=lambda args: act(args, check_daemon_status))

    # stop command
    stop_parser = subparsers.add_parser(
        'stop', help='Stop daemons.',
        formatter_class=ArgumentDefaultsAndRawDescriptionHelpFormatter)

    stop_parser.add_argument(
        'daemon', type=str, nargs='+',
        help='Daemon name', metavar='DAEMON',
        choices=set(daemon_sets.keys()))

    stop_parser.set_defaults(func=lambda args: act(args, stop_daemon))

    # start command
    restart_parser = subparsers.add_parser(
        'restart', help='Restart daemons.',
        formatter_class=ArgumentDefaultsAndRawDescriptionHelpFormatter)

    restart_parser.add_argument(
        'daemon', type=str, nargs='+',
        help='Daemon name', metavar='DAEMON',
        choices=set(daemon_sets.keys()))

    restart_parser.set_defaults(func=lambda args: act(args, restart_daemon))

    # test command
    test_parser = subparsers.add_parser(
        'test', help='Runs service directly without detaching.',
        formatter_class=ArgumentDefaultsAndRawDescriptionHelpFormatter)

    test_parser.add_argument(
        'daemon', type=str,
        help='Daemon name', metavar='DAEMON',
        choices=set(daemon_dict.keys()))

    test_parser.set_defaults(func=test_daemon)

    # config command
    config_parser = subparsers.add_parser(
        'config', help='Operate on FireWorks config.',
        formatter_class=ArgumentDefaultsAndRawDescriptionHelpFormatter)

    config_parser.add_argument(
        '--config-dir', type=str, default=FW_CONFIG_PREFIX,
        help='User config directory', metavar='CONFIG_DIR', dest='config_dir')

    config_parser.add_argument(
        '--skel-dir', type=str, default=FW_CONFIG_SKEL_PREFIX,
        help='Config skeleton directory', metavar='SKEL_DIR', dest='skel_dir')

    config_parser.add_argument(
        '--template-dir', type=str, default=FW_CONFIG_TEMPLATE_PREFIX,
        help='Config template directory', metavar='TEMPLATE_DIR',
        dest='template_dir')

    config_subparsers = config_parser.add_subparsers(
        help='sub-command', dest='command')

    # config reset command
    config_reset_parser = config_subparsers.add_parser(
        'reset',
        help=(
            "Reset FireWorks config in 'CONFIG_DIR' by first copying files "
            "from 'SKEL_DIR' and then rendering files from 'TEMPLATE_DIR' "
            "with parameters defined within your 'FWRLM_config.yaml'."),
        formatter_class=ArgumentDefaultsAndRawDescriptionHelpFormatter)

    config_reset_parser.add_argument(
        '--force', '-f', action='store_true',
        help='Force overwriting existing config.')

    config_reset_parser.set_defaults(func=reset_config)

    # config reset command
    config_show_parser = config_subparsers.add_parser(
        'show', help="Displays current config.",
        formatter_class=ArgumentDefaultsAndRawDescriptionHelpFormatter)

    config_show_parser.set_defaults(func=show_config)

    # parse
    args = parser.parse_args()

    # logging
    logformat = "%(levelname)s: %(message)s"
    if args.debug:
        logformat = (
            "[%(asctime)s-%(funcName)s()-%(filename)s:%(lineno)s]"
            " %(levelname)s: %(message)s"
        )
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
    for h in logger.handlers:
        logger.removeHandler(h)

    # create and append custom handles

    # only info & debug to stdout
    def stdout_filter(record):
        return record.levelno <= logging.INFO

    stdouth = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(logformat)
    stdouth.setFormatter(formatter)
    stdouth.setLevel(loglevel)
    stdouth.addFilter(stdout_filter)

    stderrh = logging.StreamHandler(sys.stderr)
    stderrh.setFormatter(formatter)
    stderrh.setLevel(logging.WARNING)

    logger.addHandler(stdouth)
    logger.addHandler(stderrh)

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
