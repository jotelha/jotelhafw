"""smb share fixture."""

import logging
import pytest
import requests
import time
import urllib3

from smb.SMBConnection import SMBConnection
from smb.base import NotConnectedError

from utils import _allocate_random_free_port, _random_string

SAMBA_CONTAINER_IMAGE_NAME = 'docker.io/dperson/samba:latest'
SAMBA_HOST = 'localhost'

SAMBA_WORKGROUP = 'WORKGROUP'
SAMBA_SHARE = "sambashare"


@pytest.mark.usefixtures("podman_system_service")
@pytest.fixture
def smb_share(request, podman_system_service):
    """Provide smb share (via launching samba server container)."""
    logger = logging.getLogger(__name__)

    smb_port_139 = None
    smb_port_445 = None
    samba_container_name = None
    samba_container_id = None
    request_template = podman_system_service + '/{}'

    logger.info("Check image '{}' existance.".format(SAMBA_CONTAINER_IMAGE_NAME))
    uri_suffix = 'libpod/images/{}/exists'.format(SAMBA_CONTAINER_IMAGE_NAME)
    req = request_template.format(uri_suffix)
    response = requests.get(req)
    if response.status_code == 404:
        logger.info("Image '{}' does not exist, creation not implemented.".format(SAMBA_CONTAINER_IMAGE_NAME))
        # TODO: implement image creation
        return
    elif response.status_code != 204:
        logger.warning("Unexpected error at image existance check {}: '{}'.".format(
                       response.status_code, response.text))
        return None

    smb_port_139 = _allocate_random_free_port(SAMBA_HOST)
    smb_port_445 = _allocate_random_free_port(SAMBA_HOST)
    samba_container_name = _random_string()
    req = request_template.format('libpod/containers/create')
    request_params = {
        'image': SAMBA_CONTAINER_IMAGE_NAME,
        'name': samba_container_name,
        'portmappings': [
            {
                'container_port': 139,
                'host_port': smb_port_139,
            },
            {
                'container_port': 445,
                'host_port': smb_port_445,
            }
        ],
        'command': [
            "-p", "-S", "-w", "{workgroup}".format(workgroup=SAMBA_WORKGROUP),
            "-s", "{sambashare:};/share;yes;no;yes".format(sambashare=SAMBA_SHARE)
        ]
        # refer to https://hub.docker.com/r/dperson/samba
        # -s "<name;/path>[;browse;readonly;guest;users;admins;writelist;comment]"
        #             Configure a share
        #             required arg: "<name>;</path>"
        #             <name> is how it's called for clients
        #             <path> path to share
        #             NOTE: for the default values, just leave blank
        #             [browsable] default:'yes' or 'no'
        #             [readonly] default:'yes' or 'no'
        #             [guest] allowed default:'yes' or 'no'
        #             NOTE: for user lists below, usernames are separated by ','
        #             [users] allowed default:'all' or list of allowed users
        #             [admins] allowed default:'none' or list of admin users
        #             [writelist] list of users that can write to a RO share
        #             [comment] description of share
    }
    logger.info("Create container '{}': '{}'..".format(req, request_params))
    response = requests.post(req, json=request_params)
    if response.status_code == 201:
        samba_container_id = response.json()['Id']
        logger.info("Created container '{}': '{}'.".format(
                    samba_container_id, samba_container_name))
    else:
        logger.warning("Unexpected error at container creation {}: '{}'.".format(
                       response.status_code, response.text))
        return None

    uri_suffix = 'libpod/containers/{}/start'.format(samba_container_id)
    req = request_template.format(uri_suffix)
    logger.info("Start container '{}'".format(req))
    response = requests.post(req)
    if response.status_code == 204:
        logger.info("Container '{}': '{}' started successfully.".format(
            samba_container_id, samba_container_name))
    elif response.status_code == 304:
        logger.warning("Container '{}': '{}' already running.".format(
            samba_container_id, samba_container_name))
        # stop?
    else:
        logger.warning("Unexpected error at container launch {}: '{}'.".format(
                       response.status_code, response.text))
        ret = None

    conn = SMBConnection("guest", "a-guest-needs-no-password", "pytest",
                         SAMBA_HOST, use_ntlm_v2=True)

    while(True):
        try:
            conn_success = conn.connect(SAMBA_HOST, smb_port_445)
        except ConnectionResetError as exc:
            logger.warning("Connection to smb share failed ({}), retry...".format(exc))
            time.sleep(1)
        else:
            break

    if conn_success:
        ret = conn
        shares = conn.listShares()
        share_names = [s.name for s in shares]
        logger.info("Initial shares on smb host: {}.".format(share_names))
    else:
        logger.warning("Connection to smb share failed.")

    def finalizer():
        try:
            # TODO: check on connection state and reconnect if necessary
            shares = conn.listShares()
            share_names = [s.name for s in shares]
            logger.info("Eventual shares on smb host: {}.".format(share_names))

            for share in share_names:
                try:
                    content = conn.listPath(share, '')
                except Exception as exc:
                    logger.warning("Error on listing content of '{}': {}".format(share, exc))
                else:
                    content_names = [c.filename for c in content]
                    logger.info("Top-level content of share {}: {}.".format(share, content_names))

        except Exception as exc:
            # except NotConnectedError as exc:
            logger.warning("Error on listing files on smb share: {}".format(exc))

        try:
            conn.close()
        except Exception as exc:
            logger.warning("Error on closing smb connection: {}".format(exc))


        if samba_container_id:
            # stop container
            uri_suffix = "libpod/containers/{name:}/stop".format(name=samba_container_id)
            request = request_template.format(uri_suffix)
            try:
                response = requests.post(request)
            except urllib3.exceptions.MaxRetryError as exc:
                logger.warning("Unexpected exception '{}' when sending conatiner stop request.".format(exc))
            else:
                if response.status_code == 204:
                    logger.info("Container '{}': '{}' stopped successfully.".format(
                        samba_container_id, samba_container_name))
                elif response.status_code == 304:
                    logger.warning("Container '{}': '{}' not running.".format(
                        samba_container_id, samba_container_name))
                else:
                    logger.error(
                        "Unexpected error when stopping container '{}': '{}' - {}.".format(
                            samba_container_id, samba_container_name, response.text))

            # remove container
            uri_suffix = "libpod/containers/{name:}".format(name=samba_container_id)
            try:
                request = request_template.format(uri_suffix)
            except urllib3.exceptions.MaxRetryError as exc:
                logger.warning("Unexpected exception '{}' when sending conatiner stop request.".format(exc))
            else:
                response = requests.delete(request, params={'v': True})  # v: delete attached volumes
                if response.status_code == 204:
                    logger.info("Container '{}': '{}' removed successfully.".format(
                        samba_container_id, samba_container_name))
                else:
                    logger.error(
                        "Unexpected error when stopping container '{}': '{}' - {}.".format(
                            samba_container_id, samba_container_name, response.text))

    request.addfinalizer(finalizer)
    return ret
