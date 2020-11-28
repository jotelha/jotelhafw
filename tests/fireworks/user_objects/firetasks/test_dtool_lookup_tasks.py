# coding: utf-8
"""Test dtool lookup server queries integration."""

__author__ = 'Johannes Laurin Hoermann'
__copyright__ = 'Copyright 2020, IMTEK Simulation, University of Freiburg'
__email__ = 'johannes.hoermann@imtek.uni-freiburg.de, johannes.laurin@gmail.com'
__date__ = 'Nov 05, 2020'


import logging
import pytest

from imteksimfw.utils.logging import _log_nested_dict

from imteksimfw.fireworks.user_objects.firetasks.dtool_lookup_tasks import (
    QueryDtoolTask, ReadmeDtoolTask, ManifestDtoolTask)
from imteksimfw.utils.dict import compare, _make_marker

# from test_dtool_tasks import _compare


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


@pytest.fixture
def default_readme_dtool_task_spec(dtool_lookup_config):
    """Provide default test task_spec for ReadmeDtoolTask."""
    return {
        'dtool_config': dtool_lookup_config,
        'stored_data': True,
        'uri': 'smb://test-share/1a1f9fad-8589-413e-9602-5bbd66bfe675',
        'loglevel': logging.DEBUG,
    }


@pytest.fixture
def default_manifest_dtool_task_spec(dtool_lookup_config):
    """Provide default test task_spec for ManifestDtoolTask."""
    return {
        'dtool_config': dtool_lookup_config,
        'stored_data': True,
        'uri': 'smb://test-share/1a1f9fad-8589-413e-9602-5bbd66bfe675',
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

    output = fw_action.stored_data['output']

    assert len(output) == 1

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

    compares = compare(
        output[0],
        expected_respones[0],
        to_compare
    )
    assert compares


def test_readme_dtool_task_run(dtool_lookup_server, default_readme_dtool_task_spec, dtool_lookup_config):
    """Will lookup some dataset on the server."""
    logger = logging.getLogger(__name__)

    logger.debug("Instantiate ReadmeDtoolTask with '{}'".format(
        default_readme_dtool_task_spec))

    t = ReadmeDtoolTask(**default_readme_dtool_task_spec)
    fw_action = t.run_task({})
    logger.debug("FWAction:")
    _log_nested_dict(logger.debug, fw_action.as_dict())

    output = fw_action.stored_data['output']

    # TODO: dataset creation in test
    expected_respone = {
        "creation_date": "2020-11-08",
        "description": "testing description",
        "expiration_date": "2022-11-08",
        "funders": [
          {
            "code": "testing_code",
            "organization": "testing_organization",
            "program": "testing_program"
          }
        ],
        "owners": [
          {
            "email": "testing@test.edu",
            "name": "Testing User",
            "orcid": "testing_orcid",
            "username": "testing_user"
          }
        ],
        "project": "testing project"
    }

    assert compare(output, expected_respone)


def test_manifest_dtool_task_run(dtool_lookup_server, default_manifest_dtool_task_spec, dtool_lookup_config):
    """Will lookup some dataset on the server."""
    logger = logging.getLogger(__name__)

    logger.debug("Instantiate ManifestDtoolTask with '{}'".format(
        default_manifest_dtool_task_spec))

    t = ManifestDtoolTask(**default_manifest_dtool_task_spec)
    fw_action = t.run_task({})
    logger.debug("FWAction:")
    _log_nested_dict(logger.debug, fw_action.as_dict())

    output = fw_action.stored_data['output']

    # TODO: dataset creation in test
    expected_respone = {
        "dtoolcore_version": "3.18.0",
        "hash_function": "md5sum_hexdigest",
        "items": {
            "eb58eb70ebcddf630feeea28834f5256c207edfd": {
                "hash": "2f7d9c3e0cfd47e8fcab0c12447b2bf0",
                "relpath": "simple_text_file.txt",
                "size_in_bytes": 17,
                "utc_timestamp": 1606595093.53965
             }
        }
    }

    marker = {
        "dtoolcore_version": False,
        "hash_function": "md5sum_hexdigest",
        "items": {
            "eb58eb70ebcddf630feeea28834f5256c207edfd": {
                "hash": True,
                "relpath": True,
                "size_in_bytes": True,
                "utc_timestamp": False,
             }
        }
    }

    assert compare(output, expected_respone)