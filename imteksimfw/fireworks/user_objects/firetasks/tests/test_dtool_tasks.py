# coding: utf-8

from __future__ import unicode_literals

__author__ = 'Johannes Hoermann'

import logging
import unittest
import os
import tempfile
import json
import ruamel.yaml as yaml

# needs dtool cli for verification
import subprocess

from imteksimfw.fireworks.user_objects.firetasks.dtool_tasks import CreateDatasetTask

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


class DtoolTasksTest(unittest.TestCase):
    def setUp(self):
        self.files = {
            'dtool_readme_static_and_dynamic_metadata_test': os.path.join(module_dir, "dtool_readme_static_and_dynamic_metadata_test.yml"),
            'dtool_readme_dynamic_metadata_test': os.path.join(module_dir, "dtool_readme_dynamic_metadata_test.yml"),
            'dtool_readme_static_metadata_test': os.path.join(module_dir, "dtool_readme_static_metadata_test.yml"),
            'dtool_readme_template_path': os.path.join(module_dir, "dtool_readme.yml"),
            'dtool_config_path': os.path.join(module_dir, "dtool.json"),
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
        t.run_task({})

        dtool_verify_cmd = ['dtool', 'verify', self._dataset_name]
        p = subprocess.Popen(
            dtool_verify_cmd,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        outs, errs = p.communicate()
        returncode = p.returncode

        logger.debug("'{}' returned '{}'".format(
            ' '.join(dtool_verify_cmd), returncode))
        logger.debug("'{}' stdout:".format(' '.join(dtool_verify_cmd)))
        logger.debug(outs)
        logger.debug("'{}' stderr:".format(' '.join(dtool_verify_cmd)))
        logger.debug(errs)
        self.assertEqual(returncode, 0)

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
        t.run_task({})

        dtool_verify_cmd = ['dtool', 'verify', self._dataset_name]
        p = subprocess.Popen(
            dtool_verify_cmd,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        outs, errs = p.communicate()
        returncode = p.returncode

        logger.debug("'{}' returned '{}'".format(
            ' '.join(dtool_verify_cmd), returncode))
        logger.debug("'{}' stdout:".format(' '.join(dtool_verify_cmd)))
        logger.debug(outs)
        logger.debug("'{}' stderr:".format(' '.join(dtool_verify_cmd)))
        logger.debug(errs)
        self.assertEqual(returncode, 0)

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
        t.run_task(  # insert some fw_spec into metadata
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

        dtool_verify_cmd = ['dtool', 'verify', self._dataset_name]
        p = subprocess.Popen(
            dtool_verify_cmd,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        outs, errs = p.communicate()
        returncode = p.returncode

        logger.debug("'{}' returned '{}'".format(
            ' '.join(dtool_verify_cmd), returncode))
        logger.debug("'{}' stdout:".format(' '.join(dtool_verify_cmd)))
        logger.debug(outs)
        logger.debug("'{}' stderr:".format(' '.join(dtool_verify_cmd)))
        logger.debug(errs)
        self.assertEqual(returncode, 0)

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
        t.run_task(  # insert some fw_spec into metadata
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

        dtool_verify_cmd = ['dtool', 'verify', self._dataset_name]
        p = subprocess.Popen(
            dtool_verify_cmd,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        outs, errs = p.communicate()
        returncode = p.returncode

        logger.debug("'{}' returned '{}'".format(
            ' '.join(dtool_verify_cmd), returncode))
        logger.debug("'{}' stdout:".format(' '.join(dtool_verify_cmd)))
        logger.debug(outs)
        logger.debug("'{}' stderr:".format(' '.join(dtool_verify_cmd)))
        logger.debug(errs)
        self.assertEqual(returncode, 0)

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



if __name__ == '__main__':
    unittest.main()
