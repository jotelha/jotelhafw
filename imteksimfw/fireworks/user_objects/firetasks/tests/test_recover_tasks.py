# coding: utf-8
"""Test dtool integration.

To see verbose logging during testing, run something like

    import logging
    import unittest
    from imteksimfw.fireworks.user_objects.firetasks.tests.test_recover_tasks import RecoverTasksTest
    logging.basicConfig(level=logging.DEBUG)
    suite = unittest.TestSuite()
    suite.addTest(RecoverTasksTest('test_do_nothing'))
    runner = unittest.TextTestRunner()
    runner.run(suite)
"""
__author__ = 'Johannes Laurin Hoermann'
__copyright__ = 'Copyright 2020, IMTEK Simulation, University of Freiburg'
__email__ = 'johannes.hoermann@imtek.uni-freiburg.de, johannes.laurin@gmail.com'
__date__ = 'Aug 11, 2020'

import logging
import unittest
import os
import tempfile
import json
# import ruamel.yaml as yaml

# needs dtool cli for verification
# import subprocess  # TODO: replace cli sub-processes with custom verify calls

from imteksimfw.fireworks.user_objects.firetasks.recover_tasks import RecoverTask

module_dir = os.path.abspath(os.path.dirname(__file__))


def _log_nested_dict(dct):
    logger = logging.getLogger(__name__)
    for l in json.dumps(dct, indent=2, default=str).splitlines():
        logger.debug(l)


def _read_json(file):
    with open(file, 'r') as stream:
        return json.load(stream)


# def _read_yaml(file):
#     with open(file, 'r') as stream:
#         return yaml.safe_load(stream)


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

    # source, target and marker must have same length:
    elif isinstance(marker, list):
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


class RecoverTasksTest(unittest.TestCase):
    def setUp(self):
        # logger = logging.getLogger(__name__)

        self._tmpdir = tempfile.TemporaryDirectory()
        self._previous_working_directory = os.getcwd()
        os.chdir(self._tmpdir.name)

    def tearDown(self):
        os.chdir(self._previous_working_directory)
        self._tmpdir.cleanup()
        # os.environ = self._original_environ

    def test_do_nothing(self):
        """Will run a recovery task that does nothing."""
        logger = logging.getLogger(__name__)

        task_spec = {
            'fizzle_on_no_restart_file': False,
        }
        fw_spec = {
            '_job_info': [{
                'launch_dir': os.curdir,
                'name': 'dummy parent',
                'fw_id': 1,
            }]
        }
        logger.debug("Instantiate RecoverTask with '{}'".format(task_spec))
        t = RecoverTask(**task_spec)
        logger.debug("Run with fw_spec '{}'".format(fw_spec))
        fw_action = t.run_task(fw_spec)
        logger.debug("FWAction:")
        _log_nested_dict(fw_action.as_dict())
        additions = fw_action.additions
        detours = fw_action.detours

        self.assertEqual(additions, [])
        self.assertEqual(detours, [])


if __name__ == '__main__':
    unittest.main()
