# coding: utf-8
"""Test dtool lookup server queries integration."""

__author__ = 'Johannes Laurin Hoermann'
__copyright__ = 'Copyright 2020, IMTEK Simulation, University of Freiburg'
__email__ = 'johannes.hoermann@imtek.uni-freiburg.de, johannes.laurin@gmail.com'
__date__ = 'Nov 05, 2020'


import logging
import pytest

from imteksimfw.utils.logging import _log_nested_dict

from imteksimfw.fireworks.user_objects.firetasks.dtool_lookup_tasks import QueryDtoolTask

from test_dtool_tasks import _compare


@pytest.fixture
def dtool_lookup_config(dtool_config):
    """Provide default dtool lookup config."""
    dtool_config.update({
        "DTOOL_LOOKUP_SERVER_URL": "https://localhost:5000",
        "DTOOL_LOOKUP_SERVER_TOKEN_GENERATOR_URL": "http://localhost:5001/token",
        "DTOOL_LOOKUP_SERVER_USERNAME": "testuser",
        "DTOOL_LOOKUP_SERVER_PASSWORD": "test_password",
        "DTOOL_LOOKUP_SERVER_VERIFY_SSL": False,
    })
    return dtool_config


@pytest.fixture
def default_query_dtool_task_spec(dtool_lookup_config):
    """Provide default test task_spec for QueryDtoolTask."""
    return {
        'dtool_config': dtool_lookup_config,
        'stored_data': True,
        'query': {
            'base_uri': 'smb://test-share',
            'name': {'$regex': 'test'},
        },
        'loglevel': logging.DEBUG,
    }


#
# dtool lookup tasks tests
#
def test_query_dtool_task_run(dtool_lookup_server, default_query_dtool_task_spec, dtool_lookup_config):
    """Will lookup some dataset on the server."""
    logger = logging.getLogger(__name__)

    logger.debug("Instantiate QueryDtoolTask with '{}'".format(
        default_query_dtool_task_spec))

    t = QueryDtoolTask(**default_query_dtool_task_spec)
    fw_action = t.run_task({})
    logger.debug("FWAction:")
    _log_nested_dict(logger.debug, fw_action.as_dict())

    response = fw_action.stored_data['response']

    assert len(response) == 1

    # TODO: dataset creation in test
    expected_respones = [
        {
            "base_uri": "smb://test-share",
            "created_at": "Sun, 08 Nov 2020 18:38:40 GMT",
            "creator_username": "jotelha",
            "dtoolcore_version": "3.17.0",
            "frozen_at": "Mon, 09 Nov 2020 11:33:41 GMT",
            "name": "simple_test_dataset",
            "tags": [],
            "type": "dataset",
            "uri": "smb://test-share/1a1f9fad-8589-413e-9602-5bbd66bfe675",
            "uuid": "1a1f9fad-8589-413e-9602-5bbd66bfe675"
        }
    ]

    to_compare = {
        "base_uri": True,
        "created_at": False,
        "creator_username": True,
        "dtoolcore_version": False,
        "frozen_at": False,
        "name": True,
        "tags": True,
        "type": True,
        "uri": True,
        "uuid": True
    }

    compares = _compare(
        response[0],
        expected_respones[0],
        to_compare
    )
    assert compares
