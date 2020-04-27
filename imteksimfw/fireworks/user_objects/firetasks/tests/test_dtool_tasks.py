# coding: utf-8
"""Test dtool integration.

To see verbose logging during testing, run something like

    import logging
    import unittest
    from imteksimfw.fireworks.user_objects.firetasks.tests.test_dtool_tasks import DtoolTasksTest
    logging.basicConfig(level=logging.DEBUG)
    suite = unittest.TestSuite()
    suite.addTest(DtoolTasksTest('test_create_dataset_task_run'))
    suite.addTest(DtoolTasksTest('test_static_metadata_override'))
    suite.addTest(DtoolTasksTest('test_dynamic_metadata_override'))
    suite.addTest(DtoolTasksTest('test_static_and_dynamic_metadata_override'))
    suite.addTest(DtoolTasksTest('test_copy_dataset_task_to_smb_share'))
    runner = unittest.TextTestRunner()
    runner.run(suite)
"""
__author__ = 'Johannes Laurin Hoermann'
__copyright__ = 'Copyright 2020, IMTEK Simulation, University of Freiburg'
__email__ = 'johannes.hoermann@imtek.uni-freiburg.de, johannes.laurin@gmail.com'
__date__ = 'Apr 27, 2020'

import logging
import unittest
import os
import tempfile
import json
import ruamel.yaml as yaml

# needs dtool cli for verification
import subprocess  # TODO: replace cli sub-processes with custom verify calls

import dtoolcore
from imteksimfw.fireworks.user_objects.firetasks.dtool_tasks import CreateDatasetTask, CopyDatasetTask

module_dir = os.path.abspath(os.path.dirname(__file__))


def _log_nested_dict(dct):
    logger = logging.getLogger(__name__)
    for l in json.dumps(dct, indent=2, default=str).splitlines():
        logger.debug(l)

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
    _log_nested_dict(frozen_readme)

    reference_readme = _read_yaml(template_file)
    logger.debug("Reference README.yml:")
    _log_nested_dict(reference_readme)

    dtool_config = _read_json(config_file)
    logger.debug("dtool config:")
    _log_nested_dict(dtool_config)

    logger.debug("Comaprison mode:")
    _log_nested_dict(to_compare)

    parsed_reference_readme = _inject(
        reference_readme, dtool_config, to_compare)

    logger.debug("Parsed reference README.yml:")
    _log_nested_dict(parsed_reference_readme)

    return _compare(frozen_readme, parsed_reference_readme, to_compare)


# TODO: there seems to be an issue with,
# https://github.com/jic-dtool/dtool-info/blob/64e021dc06cc6dc6df3d5858fda3e553fa18b91d/dtool_info/dataset.py#L354
# neglects custom config file
# bug report, pull, then remove here
def verify(full, dataset_uri, config_file=None):
    """Verify the integrity of a dataset.
    """
    logger = logging.getLogger(__name__)
    dataset = dtoolcore.DataSet.from_uri(dataset_uri, config_file)
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
        logger.warn(message)
        all_okay = False

    for i in manifest_identifiers.difference(generated_identifiers):
        message = "Missing item: {} {}".format(
            i,
            dataset.item_properties(i)["relpath"]
        )
        logger.warn(message)
        all_okay = False

    for i in manifest_identifiers.intersection(generated_identifiers):
        generated_size = generated_sizes[i]
        manifest_size = dataset.item_properties(i)["size_in_bytes"]
        if generated_size != manifest_size:
            message = "Altered item size: {} {}".format(
                i,
                dataset.item_properties(i)["relpath"]
            )
            logger.warn(message)
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
                logger.warn(message)
                all_okay = False

    return all_okay


class DtoolTasksTest(unittest.TestCase):
    def setUp(self):
        self.files = {
            'dtool_readme_static_and_dynamic_metadata_test':
                os.path.join(
                    module_dir,
                    "dtool_readme_static_and_dynamic_metadata_test.yml"),
            'dtool_readme_dynamic_metadata_test':
                os.path.join(
                    module_dir, "dtool_readme_dynamic_metadata_test.yml"),
            'dtool_readme_static_metadata_test':
                os.path.join(
                    module_dir, "dtool_readme_static_metadata_test.yml"),
            'dtool_readme_template_path':
                os.path.join(module_dir, "dtool_readme.yml"),
            'dtool_config_path':
                os.path.join(module_dir, "dtool.json"),
        }

        self._tmpdir = tempfile.TemporaryDirectory()
        self._previous_working_directory = os.getcwd()
        os.chdir(self._tmpdir.name)

        self._dataset_name = 'dataset'
        self.default_dtool_task_spec = {
            'name': self._dataset_name,
            'dtool_readme_template_path': self.files["dtool_readme_template_path"],
            'dtool_config_path': self.files["dtool_config_path"],
        }

    def tearDown(self):
        os.chdir(self._previous_working_directory)
        self._tmpdir.cleanup()

    def test_create_dataset_task_run(self):
        """Will create dataset with default parameters within current working directory."""
        logger = logging.getLogger(__name__)

        logger.debug("Instantiate CreateDatasetTask with '{}'".format(
            self.default_dtool_task_spec))
        t = CreateDatasetTask(**self.default_dtool_task_spec)
        fw_action = t.run_task({})
        logger.debug("FWAction:")
        _log_nested_dict(fw_action.as_dict())
        uri = fw_action.stored_data['uri']

        ret = verify(True, uri, self.files['dtool_config_path'])
        self.assertEqual(ret, True)

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
            os.path.join(self._dataset_name, "README.yml"),
            self.files['dtool_readme_template_path'],
            self.files['dtool_config_path'],
            to_compare
        )
        self.assertEqual(compares, True)

    def test_static_metadata_override(self):
        """Will create dataset with static metadata within current working directory."""
        logger = logging.getLogger(__name__)

        t = CreateDatasetTask(
            **self.default_dtool_task_spec,
            metadata={
                'metadata': {
                    'step': 'static metadata test'
                }
            }
        )
        fw_action = t.run_task({})
        logger.debug("FWAction:")
        _log_nested_dict(fw_action.as_dict())
        uri = fw_action.stored_data['uri']

        ret = verify(True, uri, self.files['dtool_config_path'])
        self.assertEqual(ret, True)

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
            os.path.join(self._dataset_name, "README.yml"),
            self.files['dtool_readme_static_metadata_test'],
            self.files['dtool_config_path'],
            to_compare
        )
        self.assertEqual(compares, True)

    def test_dynamic_metadata_override(self):
        """Will create dataset with static and dynamic metadata within current working directory."""
        logger = logging.getLogger(__name__)

        t = CreateDatasetTask(
            **self.default_dtool_task_spec,
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
        _log_nested_dict(fw_action.as_dict())
        uri = fw_action.stored_data['uri']

        ret = verify(True, uri, self.files['dtool_config_path'])
        self.assertEqual(ret, True)

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
            os.path.join(self._dataset_name, "README.yml"),
            self.files['dtool_readme_dynamic_metadata_test'],
            self.files['dtool_config_path'],
            to_compare
        )
        self.assertEqual(compares, True)

    def test_static_and_dynamic_metadata_override(self):
        """Will create dataset with static metadata within current working directory."""
        logger = logging.getLogger(__name__)

        t = CreateDatasetTask(
            **self.default_dtool_task_spec,
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
        _log_nested_dict(fw_action.as_dict())
        uri = fw_action.stored_data['uri']

        ret = verify(True, uri, self.files['dtool_config_path'])
        self.assertEqual(ret, True)

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
            os.path.join(self._dataset_name, "README.yml"),
            self.files['dtool_readme_static_and_dynamic_metadata_test'],
            self.files['dtool_config_path'],
            to_compare
        )
        self.assertEqual(compares, True)

    def test_copy_dataset_task_to_smb_share(self):
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

        # create a dummy dataset locally for transferring to share
        self.test_create_dataset_task_run()

        # configure within dtool.json
        target = 'smb://test-share'

        logger.debug("Instantiate CopyDatasetTask.")
        t = CopyDatasetTask(
            source=self._dataset_name,
            target=target,
            dtool_config_path=self.files['dtool_config_path'])

        fw_action = t.run_task({})
        logger.debug("FWAction:")
        _log_nested_dict(fw_action.as_dict())
        target_uri = fw_action.stored_data['uri']

        ret = verify(True, target_uri, self.files['dtool_config_path'])
        self.assertEqual(ret, True)

        # TODO: remove dataset from testing share


if __name__ == '__main__':
    unittest.main()
