#!/usr/bin/env python
#
# render_run.py
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
"""Quickly renders a single jinja2 template file from command line."""

import argparse
import sys  # for stdout and stderr
import logging

from imteksimfw.fireworks.fwrlm_config import config_to_dict
from imteksimfw.fireworks.fwrlm_config import FW_CONFIG_PREFIX
from imteksimfw.fireworks.fwrlm_config import FW_CONFIG_TEMPLATE_PREFIX
from imteksimfw.fireworks.utilities.render import render_single, render_batch
from imteksimfw.fireworks.utilities.render import inspect


def render_single_action(args):
    """Translate argparsed `args` to function call `render_single` from
    imteksimfw.fireworks.utilities.render"""
    logger = logging.getLogger(__name__)
    logger.info("Render template {} to output file {} with context {}".format(
        args.infile,
        args.outfile,
        args.context))

    render_single(
        args.infile,
        args.outfile,
        args.context)


def render_batch_action(args):
    """Translate argparsed `args` to function call `render_batch` from
    imteksimfw.fireworks.utilities.render"""
    logger = logging.getLogger(__name__)

    logger.info("Render tempaltes in {:s}".format(args.template_dir))
    logger.info("    to {:s}".format(args.build_dir))
    logger.debug("    with context {}".format(args.context))

    render_batch(
        args.template_dir,
        args.build_dir,
        args.context)


def render_config_action(args):
    """"Render FireWorks user config from default templates."""
    args.context = config_to_dict()
    render_batch_action(args)


def render_inspect_action(args):
    """Translate argparsed `args` to function call `inspect` from
    imteksimfw.fireworks.utilities.render"""
    logger = logging.getLogger(__name__)
    logger.info("Inspect templates in {:s}  for undefined variables".format(
        args.template_dir))
    out = inspect(args.template_dir)
    print(out)


def main():
    """Quickly render jinja2 template files from command line."""

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

    # global logging options
    parser.add_argument('--debug', default=False, required=False,
                        action='store_true', dest="debug", help='debug')
    parser.add_argument('--verbose', default=False, required=False,
                        action='store_true', dest="verbose", help='verbose')

    parser.add_argument('--log', required=False, nargs='?', dest="log",
                        default=None, const='fwrlm.log', metavar='LOG',
                        help='Write log fwrlm.log, optionally specify log name')

    subparsers = parser.add_subparsers(help='command', dest='command')

    # render single arguments
    render_single_parser = subparsers.add_parser(
        'single', help='Render single template.',
        formatter_class=ArgumentDefaultsAndRawDescriptionHelpFormatter)

    render_single_parser.add_argument(
        'infile',
        metavar='IN',
        help='Template .yaml input file')
    render_single_parser.add_argument(
        'outfile',
        metavar='OUT',
        help='Rendered .yaml output file')
    render_single_parser.add_argument(
        '--context',
        metavar='YAML',
        help="Context",
        default={'machine': 'NEMO', 'mode': 'PRODUCTION'})
    render_single_parser.set_defaults(func=render_single_action)

    # batch rendering arguments
    render_batch_parser = subparsers.add_parser(
        'batch', help='Render batch of templates.',
        formatter_class=ArgumentDefaultsAndRawDescriptionHelpFormatter)

    render_batch_parser.add_argument(
        'template_dir',
        help="Directory containing templates.", metavar='TEMPLATE_DIR',
        dest='template_dir')

    render_batch_parser.add_argument(
        'build_dir',
        help="Output directory.", metavar='BUILD_DIR',
        dest='build_dir')

    render_batch_parser.add_argument(
        '--context',
        metavar='YAML',
        help="Context",
        default={'machine': 'NEMO', 'mode': 'PRODUCTION'})

    render_batch_parser.set_defaults(func=render_batch_action)

    # config rendering arguments
    render_config_parser = subparsers.add_parser(
        'batch', help='Render FireWorks user config.',
        formatter_class=ArgumentDefaultsAndRawDescriptionHelpFormatter)

    render_config_parser.add_argument(
        'template_dir',
        help="Directory containing templates.", metavar='TEMPLATE_DIR',
        default=FW_CONFIG_TEMPLATE_PREFIX, dest='template_dir')

    render_config_parser.add_argument(
        'build_dir',
        help="Output directory.", metavar='BUILD_DIR',
        default=FW_CONFIG_PREFIX, dest='build_dir')

    render_config_parser.set_defaults(func=render_config_action)

    # inspect arguments
    render_inspect_parser = subparsers.add_parser(
        'inspect', help='Inspect set of templates.',
        formatter_class=ArgumentDefaultsAndRawDescriptionHelpFormatter)

    render_inspect_parser.add_argument(
        'template_dir',
        help="Directory containing templates.", metavar='TEMPLATE_DIR',
        default=FW_CONFIG_TEMPLATE_PREFIX, dest='template_dir')

    render_inspect_parser.set_defaults(func=render_inspect_action)

    # parse
    args = parser.parse_args()

    # simple log format for non-debug output
    logformat = "%(levelname)s: %(message)s"
    if args.debug:
        # detailed log format for debug output
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

    # log everyting desired to stdout
    stdouth = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(logformat)
    stdouth.setFormatter(formatter)
    stdouth.setLevel(loglevel)

    # always log warnings and errors to stderr
    stderrh = logging.StreamHandler(sys.stderr)
    stderrh.setFormatter(formatter)
    stderrh.setLevel(logging.WARNING)

    logger.addHandler(stdouth)
    logger.addHandler(stderrh)

    # log everything to file if desired
    if args.log:
        fh = logging.FileHandler(args.log)
        fh.setFormatter(formatter)
        fh.setLevel(loglevel)
        logger.addHandler(fh)

    if args.debug:
        loglevel = logging.DEBUG
    elif args.verbose:
        loglevel = logging.INFO
    else:
        loglevel = logging.WARNING

    logger.setLevel(loglevel)

    logger.debug("Read following armuents from command line:")
    logger.debug(args)

    if args.command is None:
        # if no command supplied, print help
        parser.print_help()
    elif 'func' not in args:
        parser.print_help()
    else:
        args.func(args)


if __name__ == '__main__':
    main()
