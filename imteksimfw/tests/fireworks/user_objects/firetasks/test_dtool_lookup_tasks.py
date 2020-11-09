# coding: utf-8
"""Test dtool lookup server queries integration."""

__author__ = 'Johannes Laurin Hoermann'
__copyright__ = 'Copyright 2020, IMTEK Simulation, University of Freiburg'
__email__ = 'johannes.hoermann@imtek.uni-freiburg.de, johannes.laurin@gmail.com'
__date__ = 'Nov 05, 2020'


import logging
import pytest

from imteksimfw.fireworks.user_objects.firetasks.dtool_lookup_tasks import DtoolLookupTask


@pytest.fixture
def default_query_dtool_task_spec(dtool_config):
    """Provide default test task_spec for QueryDtoolTask."""
    return {
        'dtool_config': dtool_config,
        'stored_data': True,
    }


#
# dtool lookup tasks tests
#
def test_query_dtool_task_run(dtool_lookup_server, dtool_config):
    """Will lookup some dataset on the server."""
    logger = logging.getLogger(__name__)

    logger.debug("Instantiate QueryDtoolTask with '{}'".format(
        default_query_dtool_task_spec))

    # second, try some direct mongo
    query = {
        'query': {
            'base_uri': 's3://snow-white',
            'readme.descripton': {'$regex': 'from queen'},
        }
    }
    logger.info(type(dtool_lookup_server))
    # r = dtool_lookup_server.post(
    #     "/mongo/query",
    #     headers=headers,
    #     data=json.dumps(query),
    #     content_type="application/json"
    # )
    # assert r.status_code == 200
    # assert len(json.loads(r.data.decode("utf-8"))) == 2
    #
    # t = CreateDatasetTask(**default_create_dataset_task_spec)
    # fw_action = t.run_task({})
    # logger.debug("FWAction:")
    # _log_nested_dict(logger.debug, fw_action.as_dict())
    # uri = fw_action.stored_data['uri']
    #
    # logger.debug("Instantiate FreezeDatasetTask with '{}'".format(
    #     {'uri': uri, **default_freeze_dataset_task_spec}))
    # t = FreezeDatasetTask(
    #     uri=uri, **default_freeze_dataset_task_spec)
    # fw_action = t.run_task({})
    # logger.debug("FWAction:")
    # _log_nested_dict(logger.debug, fw_action.as_dict())
    # uri = fw_action.stored_data['uri']
    #
    # with TemporaryOSEnviron(_read_json(files['dtool_config_path'])):
    #     ret = verify(True, uri)
    #
    # assert ret
    #
    # # compare metadata template and generated README.yml
    # # expect filled in values like this:
    # #    project: DtoolTasksTest
    # #    description: Tests on Fireworks tasks for handling dtool datasets.
    # #    owners:
    # #      - name: Dtool Tasks Test
    # #        email: dtool@imteksimfw
    # #        username: jotelha
    # #    creation_date: 2020-04-27
    # #    expiration_date: 2020-04-27
    # #    metadata:
    # #      mode: trial
    # #      step: override this
    #
    # # legend:
    # # - True compares key and value
    # # - False confirms key existance but does not compare value
    # # - str looks up value from config gile
    # to_compare = {
    #     'project': True,
    #     'description': True,
    #     'owners': [{
    #         'name': 'DTOOL_USER_FULL_NAME',
    #         'email': 'DTOOL_USER_EMAIL',
    #         'username': False,
    #     }],
    #     'creation_date': False,
    #     'expiration_date': False,
    #     'metadata': {
    #         'mode': True,
    #         'step': True,
    #     }
    # }
    #
    # compares = _compare_frozen_metadata_against_template(
    #     os.path.join(DATASET_NAME, "README.yml"),
    #     files['dtool_readme_template_path'],
    #     files['dtool_config_path'],
    #     to_compare
    # )
    # assert compares
    #
    # return uri
