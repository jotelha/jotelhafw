# coding: utf-8
"""Podman system service fixture."""

import json
import logging
import os
import pytest
import requests
import subprocess
import time

from fireworks.utilities.fw_serializers import ENCODING_PARAMS

from imteksimfw.utils.logging import _log_nested_dict

from utils import _allocate_random_free_port

PODMAN_HOST = 'localhost'
PODMAN_RESTFUL_API_VERSION = '1.40.0'
PODMAN_RESTFUL_API_REQUEST_TEMPLATE = "http://{host:}:{port:}/v{version:}"


@pytest.fixture(scope="session")
def podman_system_service(request):
    """Launch rootless podman system service."""
    logger = logging.getLogger(__name__)

    podman_port = _allocate_random_free_port(PODMAN_HOST)

    protocol_host_port = "tcp:{host:}:{port:}".format(host=PODMAN_HOST, port=podman_port)

    cmd = ['podman', 'system', 'service', protocol_host_port,
           '--log-level', 'debug', '--timeout', '0'],

    logger.debug("Launching '{}'".format(' '.join(*cmd)))
    podman = subprocess.Popen(
        *cmd,
        stdin=None,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env=os.environ, **ENCODING_PARAMS)

    # request = "http://localhost:8080/v1.40.0/libpod/info")
    request_template = PODMAN_RESTFUL_API_REQUEST_TEMPLATE.format(
        host=PODMAN_HOST, port=podman_port,
        version=PODMAN_RESTFUL_API_VERSION) + '/{}'
    req = request_template.format('libpod/info')
    while(True):
        try:
            response = requests.get(req)
        except requests.exceptions.ConnectionError as exc:
            logger.warning("Retrying after connection timeout: '{}'".format(exc))
            time.sleep(1)
        else:
            break

    if response.status_code != 200:
        logger.warning(
            "No connection via RESTful API to podman '{}: {}'".format(
                response.status_code, response.text))
        return
    info = json.loads(response.text)
    logger.info("Podman API info:")
    _log_nested_dict(logger.info, info)

    base_uri = PODMAN_RESTFUL_API_REQUEST_TEMPLATE.format(
        host=PODMAN_HOST, port=podman_port,
        version=PODMAN_RESTFUL_API_VERSION)

    def finalizer():
        """End podman system service."""
        ret = podman.poll()
        if isinstance(ret, int):
            logger.warning("podman system service returned with {}.".format(ret))
        else:  # None
            logger.info("Stopping podman system service....")
            podman.terminate()

        outs, errs = podman.communicate(timeout=3)

        logger.debug("podman system service stdout")
        for line in outs.splitlines():
            logger.debug(line)

        logger.debug("podman system service stderr")
        for line in errs.splitlines():
            logger.debug(line)

    request.addfinalizer(finalizer)
    return base_uri
