# coding: utf-8
#
# borrowed from
#   https://github.com/jic-dtool/dtool-lookup-server/blob/master/tests/__init__.py
# 05 Nov 2020
#
"""Fixtures common utility functions."""

import logging
import random
import socket
import string


def _random_string(
        size=9,
        prefix="test_",
        chars=string.ascii_uppercase + string.ascii_lowercase + string.digits):
    return prefix + ''.join(random.choice(chars) for _ in range(size))


def _allocate_random_free_port(host='localhost'):
    logger = logging.getLogger(__name__)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, 0))
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    _, local_port = s.getsockname()
    logger.info("Allocated free local port: {:d}".format(local_port))
    return local_port
