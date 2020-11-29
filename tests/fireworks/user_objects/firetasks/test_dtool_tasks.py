# coding: utf-8
"""Test dtool integration."""
__author__ = 'Johannes Laurin Hoermann'
__copyright__ = 'Copyright 2020, IMTEK Simulation, University of Freiburg'
__email__ = 'johannes.hoermann@imtek.uni-freiburg.de, johannes.laurin@gmail.com'
__date__ = 'Apr 27, 2020'

import glob
import json
import logging
import os
import pytest
import ruamel.yaml as yaml

# needs dtool cli for verification

import dtoolcore

from imteksimfw.utils.environ import TemporaryOSEnviron
from imteksimfw.utils.logging import _log_nested_dict
from imteksimfw.fireworks.user_objects.firetasks.dtool_tasks import (
    CreateDatasetTask, FreezeDatasetTask, CopyDatasetTask, FetchItemTask)

FIXTURE_DIR = os.path.abspath(os.path.dirname(__file__))

DATASET_NAME = 'dataset'


def _read_json(file):
    with open(file, 'r') as stream:
        return json.load(stream)


def _read_yaml(file):
    with open(file, 'r') as stream:
        return yaml.safe_load(stream)


def _inject(source, injection, marker):
    """Inserts values into nested dicts at positions marked by marker."""
    logger = logging.getLogger(__name__)
    if isinstance(marker, dict):
        for k, v in marker.items():
            if isinstance(v, str):  # str in marker marks desired mapping
                # inject here
                logger.debug(
                    "'{}' from injection[{}] overrides '{}' in source[{}]."
                    .format(injection[v], v, source[k], k))
                source[k] = injection[v]
            else:
                # descend
                logger.debug("Descending into sub-tree '{}' of '{}'.".format(
                    source[k], source))
                source[k] = _inject(source[k], injection, marker[k])
    elif isinstance(marker, list):  # source and marker must have same length
        logger.debug("Branching into element wise sub-trees of '{}'.".format(
            source))
        source = [_inject(s, injection, m) for s, m in zip(source, marker)]
    else:  # nothing to be done for this key
        logger.debug("No injection desired at '{}'.".format(source))

    return source


def _compare(source, target, marker):
    """Compare source and target partially, as marked by marker."""
    # ret = True
    logger = logging.getLogger(__name__)
    if isinstance(marker, dict):
        for k, v in marker.items():
            if k not in source:
                logger.error("{} not in source '{}'.".format(k, source))
                return False
            if k not in target:
                logger.error("{} not in target '{}'.".format(k, source))
                return False

            logger.debug("Descending into sub-tree '{}' of '{}'.".format(
                source[k], source))
            # descend
            if not _compare(source[k], target[k], v):
                return False  # one failed comparison suffices

    elif isinstance(marker, list):  # source, target and marker must have same length
        logger.debug("Branching into element wise sub-trees of '{}'.".format(
            source))
        for s, t, m in zip(source, target, marker):
            if not _compare(s, t, m):
                return False  # one failed comparison suffices
    else:  # arrived at leaf, comparison desired?
        if marker is not False:  # yes
            logger.debug("Comparing '{}' == '{}' -> {}.".format(
                source, target, source == target))
            return source == target

    # comparison either not desired or successfull for all elements
    return True


def _compare_frozen_metadata_against_template(
        readme_file, template_file, config_file, to_compare):
    logger = logging.getLogger(__name__)
    frozen_readme = _read_yaml(readme_file)
    logger.debug("Frozen README.yml:")
    _log_nested_dict(logger.debug, frozen_readme)

    reference_readme = _read_yaml(template_file)
    logger.debug("Reference README.yml:")
    _log_nested_dict(logger.debug, reference_readme)

    dtool_config = _read_json(config_file)
    logger.debug("dtool config:")
    _log_nested_dict(logger.debug, dtool_config)

    logger.debug("Comaprison mode:")
    _log_nested_dict(logger.debug, to_compare)

    parsed_reference_readme = _inject(
        reference_readme, dtool_config, to_compare)

    logger.debug("Parsed reference README.yml:")
    _log_nested_dict(logger.debug, parsed_reference_readme)

    return _compare(frozen_readme, parsed_reference_readme, to_compare)


# see https://github.com/jic-dtool/dtool-info/blob/64e021dc06cc6dc6df3d5858fda3e553fa18b91d/dtool_info/dataset.py#L354
def verify(full, dataset_uri):
    """Verify the integrity of a dataset."""
    logger = logging.getLogger(__name__)
    dataset = dtoolcore.DataSet.from_uri(dataset_uri)
    all_okay = True

    # Generate identifiers and sizes quickly without the
    # hash calculation used when calling dataset.generate_manifest().
    generated_sizes = {}
    generated_relpaths = {}
    for handle in dataset._storage_broker.iter_item_handles():
        identifier = dtoolcore.utils.generate_identifier(handle)
        size = dataset._storage_broker.get_size_in_bytes(handle)
        relpath = dataset._storage_broker.get_relpath(handle)
        generated_sizes[identifier] = size
        generated_relpaths[identifier] = relpath

    generated_identifiers = set(generated_sizes.keys())
    manifest_identifiers = set(dataset.identifiers)

    for i in generated_identifiers.difference(manifest_identifiers):
        message = "Unknown item: {} {}".format(
            i,
            generated_relpaths[i]
        )
        logger.warning(message)
        all_okay = False

    for i in manifest_identifiers.difference(generated_identifiers):
        message = "Missing item: {} {}".format(
            i,
            dataset.item_properties(i)["relpath"]
        )
        logger.warning(message)
        all_okay = False

    for i in manifest_identifiers.intersection(generated_identifiers):
        generated_size = generated_sizes[i]
        manifest_size = dataset.item_properties(i)["size_in_bytes"]
        if generated_size != manifest_size:
            message = "Altered item size: {} {}".format(
                i,
                dataset.item_properties(i)["relpath"]
            )
            logger.warning(message)
            all_okay = False

    if full:
        generated_manifest = dataset.generate_manifest()
        for i in manifest_identifiers.intersection(generated_identifiers):
            generated_hash = generated_manifest["items"][i]["hash"]
            manifest_hash = dataset.item_properties(i)["hash"]
            if generated_hash != manifest_hash:
                message = "Altered item hash: {} {}".format(
                    i,
                    dataset.item_properties(i)["relpath"]
                )
                logger.warning(message)
                all_okay = False

    return all_okay


#
# pytest fixtures
#
@pytest.fixture
def default_create_dataset_task_spec(dtool_config, files):
    """Provide default test task_spec for CreateDatasetTask."""
    return {
        'name': DATASET_NAME,
        'dtool_readme_template_path': files["dtool_readme_template_path"],
        'dtool_config': dtool_config,
        'stored_data': True,
    }


@pytest.fixture
def default_freeze_dataset_task_spec(dtool_config):
    """Provide default test task_spec for FreezeDatasetTask."""
    return {
        'dtool_config': dtool_config,
        'stored_data': True,
    }


@pytest.fixture
def default_copy_dataset_task_spec(dtool_config):
    """Provide default test task_spec for CopyDatasetTask."""
    return {
        'source': DATASET_NAME,
        'target': 'smb://test-share',
        'dtool_config': dtool_config,
        'stored_data': True,
    }


@pytest.fixture
def default_fetch_item_task_spec(dtool_config):
    """Provide default test task_spec for CopyDatasetTask."""
    return {
        'source': os.path.join(FIXTURE_DIR, 'immutable_test_dataset'),
        'item_id': 'eb58eb70ebcddf630feeea28834f5256c207edfd',
        'filename': 'fetched_item.txt',
        'dtool_config': dtool_config,
        'stored_data': True,
    }


def _create_dataset(tempdir, files, dtool_config,
                    default_create_dataset_task_spec,
                    default_freeze_dataset_task_spec):
    """Will create dataset with default parameters within current working directory."""
    logger = logging.getLogger(__name__)

    logger.debug("Instantiate CreateDatasetTask with '{}'".format(
        default_create_dataset_task_spec))
    t = CreateDatasetTask(**default_create_dataset_task_spec)
    fw_action = t.run_task({})
    logger.debug("FWAction:")
    _log_nested_dict(logger.debug, fw_action.as_dict())
    uri = fw_action.stored_data['uri']

    logger.debug("Instantiate FreezeDatasetTask with '{}'".format(
        {'uri': uri, **default_freeze_dataset_task_spec}))
    t = FreezeDatasetTask(
        uri=uri, **default_freeze_dataset_task_spec)
    fw_action = t.run_task({})
    logger.debug("FWAction:")
    _log_nested_dict(logger.debug, fw_action.as_dict())
    uri = fw_action.stored_data['uri']

    with TemporaryOSEnviron(_read_json(files['dtool_config_path'])):
        ret = verify(True, uri)

    assert ret

    # compare metadata template and generated README.yml
    # expect filled in values like this:
    #    project: DtoolTasksTest
    #    description: Tests on Fireworks tasks for handling dtool datasets.
    #    owners:
    #      - name: Dtool Tasks Test
    #        email: dtool@imteksimfw
    #        username: jotelha
    #    creation_date: 2020-04-27
    #    expiration_date: 2020-04-27
    #    metadata:
    #      mode: trial
    #      step: override this

    # legend:
    # - True compares key and value
    # - False confirms key existance but does not compare value
    # - str looks up value from config gile
    to_compare = {
        'project': True,
        'description': True,
        'owners': [{
            'name': 'DTOOL_USER_FULL_NAME',
            'email': 'DTOOL_USER_EMAIL',
            'username': False,
        }],
        'creation_date': False,
        'expiration_date': False,
        'metadata': {
            'mode': True,
            'step': True,
        }
    }

    compares = _compare_frozen_metadata_against_template(
        os.path.join(DATASET_NAME, "README.yml"),
        files['dtool_readme_template_path'],
        files['dtool_config_path'],
        to_compare
    )
    assert compares

    return uri


#
# basic dtool tasks tests
#
def test_create_dataset_task_run(tempdir, files, dtool_config,
                                 default_create_dataset_task_spec,
                                 default_freeze_dataset_task_spec):
    """Will create dataset with default parameters within current working directory."""
    _create_dataset(tempdir, files, dtool_config,
                    default_create_dataset_task_spec,
                    default_freeze_dataset_task_spec)


def test_static_metadata_override(tempdir, files, dtool_config,
                                  default_create_dataset_task_spec,
                                  default_freeze_dataset_task_spec):
    """Will create dataset with static metadata within current working directory."""
    logger = logging.getLogger(__name__)

    t = CreateDatasetTask(
        **default_create_dataset_task_spec,
        metadata={
            'metadata': {
                'step': 'static metadata test'
            }
        }
    )
    fw_action = t.run_task({})
    logger.debug("FWAction:")
    _log_nested_dict(logger.debug, fw_action.as_dict())
    uri = fw_action.stored_data['uri']

    logger.debug("Instantiate FreezeDatasetTask with '{}'".format(
        {'uri': uri, **default_freeze_dataset_task_spec}))
    t = FreezeDatasetTask(
        uri=uri, **default_freeze_dataset_task_spec)
    fw_action = t.run_task({})
    logger.debug("FWAction:")
    _log_nested_dict(logger.debug, fw_action.as_dict())
    uri = fw_action.stored_data['uri']

    with TemporaryOSEnviron(_read_json(files['dtool_config_path'])):
        ret = verify(True, uri)
    assert ret

    # legend:
    # - True compares key and value
    # - False confirms key existance but does not compare value
    # - str looks up value from config gile
    to_compare = {
        'project': True,
        'description': True,
        'owners': [{
            'name': 'DTOOL_USER_FULL_NAME',
            'email': 'DTOOL_USER_EMAIL',
            'username': False,
        }],
        'creation_date': False,
        'expiration_date': False,
        'metadata': {
            'mode': True,
            'step': True,
        }
    }

    compares = _compare_frozen_metadata_against_template(
        os.path.join(DATASET_NAME, "README.yml"),
        files['dtool_readme_static_metadata_test'],
        files['dtool_config_path'],
        to_compare
    )
    assert compares

    return uri


def test_dynamic_metadata_override(tempdir, files, dtool_config,
                                   default_create_dataset_task_spec,
                                   default_freeze_dataset_task_spec):
    """Will create dataset with static and dynamic metadata within current working directory."""
    logger = logging.getLogger(__name__)

    t = CreateDatasetTask(
        **default_create_dataset_task_spec,
        metadata_key='deeply->deeply->nested',
    )
    fw_action = t.run_task(  # insert some fw_spec into metadata
        {
            'deeply': {
                'deeply': {
                    'nested': {
                        'metadata': {
                            'step': 'dynamic metadata test'
                        }
                    }
                }
            }
        }
    )
    logger.debug("FWAction:")
    _log_nested_dict(logger.debug, fw_action.as_dict())
    uri = fw_action.stored_data['uri']

    logger.debug("Instantiate FreezeDatasetTask with '{}'".format(
        {'uri': uri, **default_freeze_dataset_task_spec}))
    t = FreezeDatasetTask(
        uri=uri, **default_freeze_dataset_task_spec)
    fw_action = t.run_task({})
    logger.debug("FWAction:")
    _log_nested_dict(logger.debug, fw_action.as_dict())
    uri = fw_action.stored_data['uri']

    with TemporaryOSEnviron(_read_json(files['dtool_config_path'])):
        ret = verify(True, uri)
    assert ret

    # legend:
    # - True compares key and value
    # - False confirms key existance but does not compare value
    # - str looks up value from config gile
    to_compare = {
        'project': True,
        'description': True,
        'owners': [{
            'name': 'DTOOL_USER_FULL_NAME',
            'email': 'DTOOL_USER_EMAIL',
            'username': False,
        }],
        'creation_date': False,
        'expiration_date': False,
        'metadata': {
            'mode': True,
            'step': True,
        }
    }

    compares = _compare_frozen_metadata_against_template(
        os.path.join(DATASET_NAME, "README.yml"),
        files['dtool_readme_dynamic_metadata_test'],
        files['dtool_config_path'],
        to_compare
    )
    assert compares

    return uri


def test_static_and_dynamic_metadata_override(tempdir, files, dtool_config,
                                              default_create_dataset_task_spec,
                                              default_freeze_dataset_task_spec):
    """Will create dataset with static metadata within current working directory."""
    logger = logging.getLogger(__name__)

    t = CreateDatasetTask(
        **default_create_dataset_task_spec,
        metadata={
            'metadata': {  # insert some task-level specs into metadata
                'step': 'static and dynamic metadata test',  #
                'static_field': 'set by task-level static metadata field'
            }
        },
        metadata_key='deeply->deeply->nested',
    )
    fw_action = t.run_task(  # insert some fw_spec into metadata
        {
            'deeply': {
                'deeply': {
                    'nested': {
                        'metadata': {
                            'step': 'static field above overrides me',
                            'dynamic_field': 'set by Fireworks fw_spec-level dynamic metadata field'
                        }
                    }
                }
            }
        }
    )
    logger.debug("FWAction:")
    _log_nested_dict(logger.debug, fw_action.as_dict())
    uri = fw_action.stored_data['uri']

    logger.debug("Instantiate FreezeDatasetTask with '{}'".format(
        {'uri': uri, **default_freeze_dataset_task_spec}))
    t = FreezeDatasetTask(
        uri=uri, **default_freeze_dataset_task_spec)
    fw_action = t.run_task({})
    logger.debug("FWAction:")
    _log_nested_dict(logger.debug, fw_action.as_dict())
    uri = fw_action.stored_data['uri']

    with TemporaryOSEnviron(_read_json(files['dtool_config_path'])):
        ret = verify(True, uri)
    assert ret

    # legend:
    # - True compares key and value
    # - False confirms key existance but does not compare value
    # - str looks up value from config gile
    to_compare = {
        'project': True,
        'description': True,
        'owners': [{
            'name': 'DTOOL_USER_FULL_NAME',
            'email': 'DTOOL_USER_EMAIL',
            'username': False,
        }],
        'creation_date': False,
        'expiration_date': False,
        'metadata': {
            'mode': True,
            'step': True,
            'static_field': True,
            'dynamic_field': True,
        }
    }

    compares = _compare_frozen_metadata_against_template(
        os.path.join(DATASET_NAME, "README.yml"),
        files['dtool_readme_static_and_dynamic_metadata_test'],
        files['dtool_config_path'],
        to_compare
    )
    assert compares

    return uri


def test_params_from_fw_spec(tempdir, files, dtool_config,
                             default_create_dataset_task_spec,
                             default_freeze_dataset_task_spec):
    """Will create dataset with some task parameters pulled from fw_spec."""
    logger = logging.getLogger(__name__)

    t_spec = {
        **default_create_dataset_task_spec,
        'creator_username': {'key': 'deeply->deeply->nested->username'}
    }
    t = CreateDatasetTask(**t_spec)

    fw_action = t.run_task(
        {
            'deeply': {
                'deeply': {
                    'nested': {
                        'username': 'unittest'
                    }
                }
            }
        }
    )
    logger.debug("FWAction:")
    _log_nested_dict(logger.debug, fw_action.as_dict())
    uri = fw_action.stored_data['uri']

    logger.debug("Instantiate FreezeDatasetTask with '{}'".format(
        {'uri': uri, **default_freeze_dataset_task_spec}))
    t = FreezeDatasetTask(
        uri=uri, **default_freeze_dataset_task_spec)
    fw_action = t.run_task({})
    logger.debug("FWAction:")
    _log_nested_dict(logger.debug, fw_action.as_dict())
    uri = fw_action.stored_data['uri']

    with TemporaryOSEnviron(_read_json(files['dtool_config_path'])):
        ret = verify(True, uri)
    assert ret

    # check creator_username
    dataset = dtoolcore.DataSet.from_uri(uri)
    assert dataset._admin_metadata["creator_username"] == 'unittest'

    return uri


#
# derived datasets tests
#
def test_create_derived_dataset(tempdir, files, dtool_config,
                                default_create_dataset_task_spec,
                                default_freeze_dataset_task_spec):
    """Create two datasets, one derived from the other."""
    logger = logging.getLogger(__name__)

    # create a dummy dataset to derive from
    source_dataset_uri = _create_dataset(tempdir, files, dtool_config,
                                         default_create_dataset_task_spec,
                                         default_freeze_dataset_task_spec)
    logger.debug("Derive from source dataset with URI '{}'.".format(
        source_dataset_uri))

    t_spec = default_create_dataset_task_spec.copy()
    t_spec['name'] = 'derived_datset'
    t_spec['source_dataset_uri'] = source_dataset_uri

    logger.debug("Instantiate another CreateDatasetTask with task spec:")
    _log_nested_dict(logger.debug, t_spec)
    t = CreateDatasetTask(**t_spec)
    fw_action = t.run_task({})
    logger.debug("FWAction:")
    _log_nested_dict(logger.debug, fw_action.as_dict())
    derived_datset_uri = fw_action.stored_data['uri']

    logger.debug("Instantiate FreezeDatasetTask with '{}'".format(
        {'uri': derived_datset_uri, **default_freeze_dataset_task_spec}))
    t = FreezeDatasetTask(
        uri=derived_datset_uri, **default_freeze_dataset_task_spec)
    fw_action = t.run_task({})
    logger.debug("FWAction:")
    _log_nested_dict(logger.debug, fw_action.as_dict())
    derived_dataset_uri = fw_action.stored_data['uri']

    with TemporaryOSEnviron(_read_json(files['dtool_config_path'])):
        ret = verify(True, derived_datset_uri)
    assert ret

    # check that fields for derived datasets agree
    source_dataset = dtoolcore.DataSet.from_uri(source_dataset_uri)
    derived_dataset = dtoolcore.DataSet.from_uri(derived_dataset_uri)
    assert source_dataset.name == derived_dataset.get_annotation("source_dataset_name")
    assert source_dataset.uri == derived_dataset.get_annotation("source_dataset_uri")
    assert source_dataset.uuid == derived_dataset.get_annotation("source_dataset_uuid")


def test_create_derived_dataset_from_multiple_source_dataset_uris(tempdir, files, dtool_config,
                                                                  default_create_dataset_task_spec,
                                                                  default_freeze_dataset_task_spec):
    """Create three datasets, one derived from the other two."""
    logger = logging.getLogger(__name__)

    # create two dummy datasets to derive from

    logger.debug("Instantiate CreateDatasetTask with '{}'".format(
        default_create_dataset_task_spec))

    dummy_names = ['dataset', 'another_dataset']

    # for testing, add a non-existant source uri in the beginning
    source_dataset_ref = [{'uri': 'file://some/none-existant/dataset'}]
    for name in dummy_names:
        t_spec = default_create_dataset_task_spec.copy()
        t_spec['name'] = name
        t = CreateDatasetTask(**t_spec)
        fw_action = t.run_task({})
        logger.debug("FWAction:")
        _log_nested_dict(logger.debug, fw_action.as_dict())
        uri = fw_action.stored_data['uri']
        # source_dataset_uri.append(fw_action.stored_data['uri'])

        logger.debug("Instantiate FreezeDatasetTask with '{}'".format(
            {'uri': uri, **default_freeze_dataset_task_spec}))
        t = FreezeDatasetTask(
            uri=uri, **default_freeze_dataset_task_spec)
        fw_action = t.run_task({})
        logger.debug("FWAction:")
        _log_nested_dict(logger.debug, fw_action.as_dict())
        # uri = fw_action.stored_data['uri']

        with TemporaryOSEnviron(_read_json(files['dtool_config_path'])):
            ret = verify(True, uri)

        assert ret

        to_compare = {
            'project': True,
            'description': True,
            'owners': [{
                'name': 'DTOOL_USER_FULL_NAME',
                'email': 'DTOOL_USER_EMAIL',
                'username': False,
            }],
            'creation_date': False,
            'expiration_date': False,
            'metadata': {
                'mode': True,
                'step': True,
            }
        }

        compares = _compare_frozen_metadata_against_template(
            os.path.join(DATASET_NAME, "README.yml"),
            files['dtool_readme_template_path'],
            files['dtool_config_path'],
            to_compare
        )
        assert compares
        source_dataset_ref.append(
            {
                'name': fw_action.stored_data['name'],
                'uri': fw_action.stored_data['uri'],
                'uuid': fw_action.stored_data['uuid'],

            }
        )

    logger.debug("Derive from source datasets with URIs '{}'.".format(
        source_dataset_ref))

    t_spec = default_create_dataset_task_spec.copy()
    t_spec['name'] = 'derived_datset'
    t_spec['source_dataset_uri'] = [d['uri'] for d in source_dataset_ref]

    logger.debug("Instantiate another CreateDatasetTask with task spec:")
    _log_nested_dict(logger.debug, t_spec)
    t = CreateDatasetTask(**t_spec)
    fw_action = t.run_task({})
    logger.debug("FWAction:")
    _log_nested_dict(logger.debug, fw_action.as_dict())
    derived_datset_uri = fw_action.stored_data['uri']

    logger.debug("Instantiate FreezeDatasetTask with '{}'".format(
        {'uri': derived_datset_uri, **default_freeze_dataset_task_spec}))
    t = FreezeDatasetTask(
        uri=derived_datset_uri, **default_freeze_dataset_task_spec)
    fw_action = t.run_task({})
    logger.debug("FWAction:")
    _log_nested_dict(logger.debug, fw_action.as_dict())
    derived_dataset_uri = fw_action.stored_data['uri']

    with TemporaryOSEnviron(_read_json(files['dtool_config_path'])):
        ret = verify(True, derived_datset_uri)
    assert ret

    # check that fields for derived datasets agree
    # if handled corrctly, then the second uri in the list must have
    # been picked as primary source
    source_dataset = dtoolcore.DataSet.from_uri(source_dataset_ref[1]['uri'])
    derived_dataset = dtoolcore.DataSet.from_uri(derived_dataset_uri)
    assert source_dataset.name == derived_dataset.get_annotation("source_dataset_name")
    assert source_dataset.uri == derived_dataset.get_annotation("source_dataset_uri")
    assert source_dataset.uuid == derived_dataset.get_annotation("source_dataset_uuid")

    # make sure dependeny annotations are set correctly
    source_datasets = derived_dataset.get_annotation("derived_from")
    for ref, cur in zip(source_dataset_ref, source_datasets):
        assert len(ref) == len(cur)
        for k, l in zip(sorted(ref), sorted(cur)):
            assert k == l
            assert ref[k] == cur[l]


def test_create_derived_dataset_from_multiple_source_datasets(tempdir, files, dtool_config,
                                                              default_create_dataset_task_spec,
                                                              default_freeze_dataset_task_spec):
    """Create three datasets, one derived from the other two."""
    logger = logging.getLogger(__name__)

    # create two dummy datasets to derive from

    logger.debug("Instantiate CreateDatasetTask with '{}'".format(
        default_create_dataset_task_spec))

    dummy_names = ['dataset', 'another_dataset']

    # for testing, add a non-existant source uri in the beginning
    source_dataset_ref = [{'uri': 'file://some/none-existant/dataset'}]
    for name in dummy_names:
        t_spec = default_create_dataset_task_spec.copy()
        t_spec['name'] = name
        t = CreateDatasetTask(**t_spec)
        fw_action = t.run_task({})
        logger.debug("FWAction:")
        _log_nested_dict(logger.debug, fw_action.as_dict())
        uri = fw_action.stored_data['uri']

        logger.debug("Instantiate FreezeDatasetTask with '{}'".format(
            {'uri': uri, **default_freeze_dataset_task_spec}))
        t = FreezeDatasetTask(
            uri=uri, **default_freeze_dataset_task_spec)
        fw_action = t.run_task({})
        logger.debug("FWAction:")
        _log_nested_dict(logger.debug, fw_action.as_dict())
        # uri = fw_action.stored_data['uri']

        with TemporaryOSEnviron(_read_json(files['dtool_config_path'])):
            ret = verify(True, uri)

        assert ret

        to_compare = {
            'project': True,
            'description': True,
            'owners': [{
                'name': 'DTOOL_USER_FULL_NAME',
                'email': 'DTOOL_USER_EMAIL',
                'username': False,
            }],
            'creation_date': False,
            'expiration_date': False,
            'metadata': {
                'mode': True,
                'step': True,
            }
        }

        compares = _compare_frozen_metadata_against_template(
            os.path.join(DATASET_NAME, "README.yml"),
            files['dtool_readme_template_path'],
            files['dtool_config_path'],
            to_compare
        )
        assert compares
        source_dataset_ref.append(
            {
                'name': fw_action.stored_data['name'],
                'uri': fw_action.stored_data['uri'],
                'uuid': fw_action.stored_data['uuid'],

            }
        )

    logger.debug("Derive from source datasets with URIs '{}'.".format(
        source_dataset_ref))

    t_spec = default_create_dataset_task_spec.copy()
    t_spec['name'] = 'derived_datset'
    t_spec['source_dataset'] = source_dataset_ref

    logger.debug("Instantiate another CreateDatasetTask with task spec:")
    _log_nested_dict(logger.debug, t_spec)
    t = CreateDatasetTask(**t_spec)
    fw_action = t.run_task({})
    logger.debug("FWAction:")
    _log_nested_dict(logger.debug, fw_action.as_dict())
    derived_datset_uri = fw_action.stored_data['uri']

    logger.debug("Instantiate FreezeDatasetTask with '{}'".format(
        {'uri': derived_datset_uri, **default_freeze_dataset_task_spec}))
    t = FreezeDatasetTask(
        uri=derived_datset_uri, **default_freeze_dataset_task_spec)
    fw_action = t.run_task({})
    logger.debug("FWAction:")
    _log_nested_dict(logger.debug, fw_action.as_dict())
    derived_dataset_uri = fw_action.stored_data['uri']

    with TemporaryOSEnviron(_read_json(files['dtool_config_path'])):
        ret = verify(True, derived_datset_uri)
    assert ret

    # check that fields for derived datasets agree
    # if handled corrctly, then the second uri in the list must have
    # been picked as primary source
    source_dataset = dtoolcore.DataSet.from_uri(source_dataset_ref[1]['uri'])
    derived_dataset = dtoolcore.DataSet.from_uri(derived_dataset_uri)
    assert source_dataset.name == derived_dataset.get_annotation("source_dataset_name")
    assert source_dataset.uri == derived_dataset.get_annotation("source_dataset_uri")
    assert source_dataset.uuid == derived_dataset.get_annotation("source_dataset_uuid")

    # make sure dependeny annotations are set correctly
    source_datasets = derived_dataset.get_annotation("derived_from")
    for ref, cur in zip(source_dataset_ref, source_datasets):
        assert len(ref) == len(cur)
        for k, l in zip(sorted(ref), sorted(cur)):
            assert k == l
            assert ref[k] == cur[l]


#
# copy datasets tests
#
# @pytest.mark.usefixtures("smb_share")
def test_copy_dataset_task_to_smb_share(tempdir, files, dtool_config,
                                        default_create_dataset_task_spec,
                                        default_freeze_dataset_task_spec,
                                        default_copy_dataset_task_spec,
                                        smb_share):
    """Requires a guest-writable share named 'sambashare' available locally.

    The share has to offer an empty sub-directory 'dtool'.

    A snippet within /ect/samba/smb.conf for tetsing purposes may look like
    this:

        [sambashare]
            comment = Samba on Ubuntu
            path = /tmp/sambashare
            guest ok = yes
            available = yes
            read only = no
            writeable = yes
            browsable = yes
            create mask = 0664
            directory mask = 0775
            guest account = unix-user-with-write-privilieges
            force user = unix-user-with-write-privilieges
            force group = sambashare

    You may as well modify access parameters within 'dtool.json' config.
    """
    logger = logging.getLogger(__name__)

    if smb_share is None:
        pytest.skip("No smb share provided.")

    username = smb_share.username
    password = smb_share.password
    host_name = smb_share.remote_name
    host_ip, port = smb_share.sock.getpeername()

    smb_share.createDirectory("sambashare", "dtool")

    # create a dummy dataset locally for transferring to share
    _create_dataset(tempdir, files, dtool_config,
                    default_create_dataset_task_spec,
                    default_freeze_dataset_task_spec)

    dynamic_dtool_config_overrides = {
         "DTOOL_SMB_SERVER_NAME_test-share": host_name,
         "DTOOL_SMB_SERVER_PORT_test-share": port,
         "DTOOL_SMB_USERNAME_test-share": username,
         "DTOOL_SMB_PASSWORD_test-share": password,
         "DTOOL_SMB_DOMAIN_test-share": "WORKGROUP",  # TODO extract
         "DTOOL_SMB_SERVICE_NAME_test-share": "sambashare",
         "DTOOL_SMB_PATH_test-share": "dtool"
    }
    dtool_config.update(dynamic_dtool_config_overrides)
    default_copy_dataset_task_spec['dtool_config'].update(dynamic_dtool_config_overrides)

    logger.debug("dtool_config smb overrides:")
    _log_nested_dict(logger.debug, dtool_config)

    logger.debug("default_copy_dataset_task_spec smb overrides:")
    _log_nested_dict(logger.debug, default_copy_dataset_task_spec)

    logger.debug("Instantiate CopyDatasetTask.")
    t = CopyDatasetTask(**default_copy_dataset_task_spec)

    fw_action = t.run_task({})
    logger.debug("FWAction:")
    _log_nested_dict(logger.debug, fw_action.as_dict())
    target_uri = fw_action.stored_data['uri']

    with TemporaryOSEnviron(dtool_config):
        ret = verify(True, target_uri)
    assert ret

    return target_uri


def test_fetch_item_task(tempdir, files, dtool_config,
                         default_fetch_item_task_spec):
    logger = logging.getLogger(__name__)

    # create a dummy dataset locally for extracting item
    # _create_dataset(tempdir, files, dtool_config,
    #                default_create_dataset_task_spec,
    #                default_freeze_dataset_task_spec)


    logger.debug("Instantiate FetchItemTask.")
    t = FetchItemTask(**default_fetch_item_task_spec)

    fw_action = t.run_task({})
    logger.debug("FWAction:")
    _log_nested_dict(logger.debug, fw_action.as_dict())
    source_uri = fw_action.stored_data['uri']
    with TemporaryOSEnviron(dtool_config):
        ret = verify(True, source_uri)
    assert ret

    local_item_files = glob.glob('*.txt')
    assert len(local_item_files)  == 1
    assert local_item_files[0] == 'fetched_item.txt'
    with open('fetched_item.txt', 'r') as f:
        content = f.read ()
    assert content == 'Some test content'

    return source_uri