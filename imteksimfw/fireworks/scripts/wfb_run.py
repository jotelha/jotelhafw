#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Workflow Builder, facilitates workflow construction from yaml templates."""
import logging

from imteksimfw.fireworks.utilities.wfb import build_wf
def main():
    global logger

    import argparse

    parser = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('system',
        help='.yaml input file.', default='system.yaml', metavar='system.yaml')
    parser.add_argument('build',
        help='output directory.', default='build', metavar='build-directory')
    parser.add_argument('--template-dir',
        help="Directory containing templates.",
        default="templates", dest='templates', metavar='template-directory')
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

    logging.basicConfig(level=loglevel)
    logger.setLevel(loglevel)

    logger.info("Build workflow from system desscription {} within output directory {} based on templates under {}".format(
        args.system,
        args.build,
        args.templates ) )
    build_wf(
        args.system,
        args.build,
        args.templates )

if __name__ == '__main__':
    main()
