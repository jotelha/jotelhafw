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

import glob
import json
import logging
import os
import sys
import tempfile
import unittest

# needs dtool cli for verification
# import subprocess  # TODO: replace cli sub-processes with custom verify calls

from fireworks.core.firework import Firework, Workflow
from fireworks.user_objects.firetasks.script_task import PyTask
from imteksimfw.fireworks.user_objects.firetasks.recover_tasks import RecoverTask, dict_merge

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


def _log_dir(directory=os.curdir):
    """Log content of directory subtree."""
    logger = logging.getLogger(__name__)
    for root, dirs, files in os.walk(directory, topdown=True):
        for name in files:
            logger.debug(os.path.join(root, name))
        for name in dirs:
            logger.debug(os.path.join(root, name))


class RecoverTasksTest(unittest.TestCase):
    def setUp(self):

        self.default_task_spec = {
            'loglevel': logging.DEBUG,
            'store_stdlog': True,
            'stored_data': True,
        }

        self.default_fw_spec = {}

        self._tmpdir = tempfile.TemporaryDirectory()
        self._previous_working_directory = os.getcwd()

        os.chdir(self._tmpdir.name)

    def tearDown(self):
        os.chdir(self._previous_working_directory)
        self._tmpdir.cleanup()

    def test_do_nothing(self):
        """Will run a recovery task that does nothing."""
        logger = logging.getLogger(__name__)
        logger.info("### {} ###".format(sys._getframe().f_code.co_name))

        task_spec = {
            'fizzle_on_no_restart_file': False,
        }
        task_spec = dict_merge(self.default_task_spec, task_spec)

        fw_spec = {
            '_job_info': [{
                'launch_dir': os.curdir,
                'name': 'dummy parent',
                'fw_id': 1,
            }]
        }
        fw_spec = dict_merge(self.default_fw_spec, fw_spec)

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

    def test_fizzle_on_no_restart_file(self):
        """Will run a recovery task that fizzles on no restart file."""
        logger = logging.getLogger(__name__)
        logger.info("### {} ###".format(sys._getframe().f_code.co_name))

        task_spec = {
            'fizzle_on_no_restart_file': True,
        }
        task_spec = dict_merge(self.default_task_spec, task_spec)

        fw_spec = {
            '_job_info': [{
                'launch_dir': os.curdir,
                'name': 'dummy parent',
                'fw_id': 1,
            }]
        }
        fw_spec = dict_merge(self.default_fw_spec, fw_spec)
        logger.debug("Instantiate RecoverTask with '{}'".format(task_spec))
        t = RecoverTask(**task_spec)
        logger.debug("Run with fw_spec '{}'".format(fw_spec))

        with self.assertRaises(ValueError) as cm:
            fw_action = t.run_task(fw_spec)

        self.assertEqual(str(cm.exception),
                         'No restart file in . for glob pattern *.restart[0-9]')


    def test_recover_single_restart_file(self):
        """Will run a recovery task that does nothing."""
        logger = logging.getLogger(__name__)
        logger.info("### {} ###".format(sys._getframe().f_code.co_name))

        os.mkdir('dummy_prev_launch')
        with open(os.path.join('dummy_prev_launch', 'dummy_restart_file'), 'w') as fp:
            pass

        logger.debug("Test directory tree before running RecoverTask.")
        _log_dir()

        task_spec = {
            'fizzle_on_no_restart_file': True,
            'restart_file_glob_patterns': '*_restart_file'
        }
        task_spec = dict_merge(self.default_task_spec, task_spec)

        fw_spec = {
            '_job_info': [{
                'launch_dir': 'dummy_prev_launch',
                'name': 'dummy parent',
                'fw_id': 1,
            }]
        }
        fw_spec = dict_merge(self.default_fw_spec, fw_spec)
        logger.debug("Instantiate RecoverTask with '{}'".format(task_spec))
        t = RecoverTask(**task_spec)
        logger.debug("Run with fw_spec '{}'".format(fw_spec))
        fw_action = t.run_task(fw_spec)
        logger.debug("FWAction:")
        _log_nested_dict(fw_action.as_dict())
        logger.debug("Test directory tree after running RecoverTask.")
        _log_dir()

        recoverd_restart_files = glob.glob('*_restart_file')
        self.assertEqual(len(recoverd_restart_files), 1)
        self.assertEqual(recoverd_restart_files[0], "dummy_restart_file")

    def test_select_latest_restart_file(self):
        """Will run a recovery task that does nothing."""
        logger = logging.getLogger(__name__)
        logger.info("### {} ###".format(sys._getframe().f_code.co_name))

        os.mkdir('dummy_prev_launch')
        first_restart_file = os.path.join('dummy_prev_launch', 'first_restart_file')
        second_restart_file = os.path.join('dummy_prev_launch', 'second_restart_file')
        with open(first_restart_file, 'w'):
            pass
        with open(second_restart_file, 'w'):
            pass

        logger.debug("Test directory tree before running RecoverTask.")
        _log_dir()

        # make sure files are sortable by mtime
        second_restart_file_stats = os.stat(second_restart_file)

        os.utime(first_restart_file, ns=(
            second_restart_file_stats.st_atime_ns-1e9,
            second_restart_file_stats.st_mtime_ns-1e9))

        first_restart_file_stats = os.stat(first_restart_file)

        logger.debug("firts_restart_file (atime_ns, mtime_ns) = ({}, {})".format(
                      first_restart_file_stats.st_atime_ns,
                      first_restart_file_stats.st_mtime_ns,))
        logger.debug("second_restart_file (atime_ns, mtime_ns) = ({}, {})".format(
                      second_restart_file_stats.st_atime_ns,
                      second_restart_file_stats.st_mtime_ns,))

        task_spec = {
            'fizzle_on_no_restart_file': True,
            'restart_file_glob_patterns': '*_restart_file'
        }
        task_spec = dict_merge(self.default_task_spec, task_spec)

        fw_spec = {
            '_job_info': [{
                'launch_dir': 'dummy_prev_launch',
                'name': 'dummy parent',
                'fw_id': 1,
            }]
        }
        fw_spec = dict_merge(self.default_fw_spec, fw_spec)
        logger.debug("Instantiate RecoverTask with '{}'".format(task_spec))
        t = RecoverTask(**task_spec)
        logger.debug("Run with fw_spec '{}'".format(fw_spec))
        fw_action = t.run_task(fw_spec)
        logger.debug("FWAction:")
        _log_nested_dict(fw_action.as_dict())
        logger.debug("Test directory tree after running RecoverTask.")
        _log_dir()

        recoverd_restart_files = glob.glob('*_restart_file')
        self.assertEqual(len(recoverd_restart_files), 1)
        self.assertEqual(recoverd_restart_files[0], "second_restart_file")

    def test_select_multiple_restart_files(self):
        """Will run a recovery task that does nothing."""
        logger = logging.getLogger(__name__)
        logger.info("### {} ###".format(sys._getframe().f_code.co_name))

        os.mkdir('dummy_prev_launch')

        first_restart_file = os.path.join('dummy_prev_launch', 'first_restart_file')
        second_restart_file = os.path.join('dummy_prev_launch', 'second_restart_file')
        with open(first_restart_file, 'w'):
            pass
        with open(second_restart_file, 'w'):
            pass

        logger.debug("Test directory tree before running RecoverTask.")
        _log_dir()

        # make sure files are sortable by mtime
        second_restart_file_stats = os.stat(second_restart_file)

        os.utime(first_restart_file, ns=(
            second_restart_file_stats.st_atime_ns-1e9,
            second_restart_file_stats.st_mtime_ns-1e9))

        first_restart_file_stats = os.stat(first_restart_file)

        logger.debug("firts_restart_file (atime_ns, mtime_ns) = ({}, {})".format(
                      first_restart_file_stats.st_atime_ns,
                      first_restart_file_stats.st_mtime_ns,))
        logger.debug("second_restart_file (atime_ns, mtime_ns) = ({}, {})".format(
                      second_restart_file_stats.st_atime_ns,
                      second_restart_file_stats.st_mtime_ns,))

        # different set of restart files
        first_checkpoint_file = os.path.join('dummy_prev_launch', 'first_checkpoint_file')
        second_checkpoint_file = os.path.join('dummy_prev_launch', 'second_checkpoint_file')
        with open(first_checkpoint_file, 'w'):
            pass
        with open(second_checkpoint_file, 'w'):
            pass

        logger.debug("Test directory tree before running RecoverTask.")
        _log_dir()

        # make sure files are sortable by mtime
        second_checkpoint_file_stats = os.stat(second_checkpoint_file)

        os.utime(first_checkpoint_file, ns=(
            second_checkpoint_file_stats.st_atime_ns-1e9,
            second_checkpoint_file_stats.st_mtime_ns-1e9))

        first_checkpoint_file_stats = os.stat(first_checkpoint_file)

        logger.debug("firts_checkpoint_file (atime_ns, mtime_ns) = ({}, {})".format(
                      first_checkpoint_file_stats.st_atime_ns,
                      first_checkpoint_file_stats.st_mtime_ns,))
        logger.debug("second_checkpoint_file (atime_ns, mtime_ns) = ({}, {})".format(
                      second_checkpoint_file_stats.st_atime_ns,
                      second_checkpoint_file_stats.st_mtime_ns,))

        task_spec = {
            'fizzle_on_no_restart_file': True,
            'restart_file_glob_patterns': ['*_restart_file', '*_checkpoint_file'],
        }
        task_spec = dict_merge(self.default_task_spec, task_spec)

        fw_spec = {
            '_job_info': [{
                'launch_dir': 'dummy_prev_launch',
                'name': 'dummy parent',
                'fw_id': 1,
            }]
        }
        fw_spec = dict_merge(self.default_fw_spec, fw_spec)
        logger.debug("Instantiate RecoverTask with '{}'".format(task_spec))
        t = RecoverTask(**task_spec)
        logger.debug("Run with fw_spec '{}'".format(fw_spec))
        fw_action = t.run_task(fw_spec)
        logger.debug("FWAction:")
        _log_nested_dict(fw_action.as_dict())
        logger.debug("Test directory tree after running RecoverTask.")
        _log_dir()

        recoverd_restart_files = glob.glob('*_restart_file')
        self.assertEqual(len(recoverd_restart_files), 1)
        self.assertEqual(recoverd_restart_files[0], "second_restart_file")

        recoverd_checkpoint_files = glob.glob('*_checkpoint_file')
        self.assertEqual(len(recoverd_checkpoint_files), 1)
        self.assertEqual(recoverd_checkpoint_files[0], "second_checkpoint_file")

    def test_recover_other_files(self):
        """Will run a recovery task that recovers all files matching other_glob_patterns."""
        logger = logging.getLogger(__name__)
        logger.info("### {} ###".format(sys._getframe().f_code.co_name))

        os.mkdir('dummy_prev_launch')

        some_file = os.path.join('dummy_prev_launch', 'some_file')
        some_other_file = os.path.join('dummy_prev_launch', 'some_other_file')
        some_third_file = os.path.join('dummy_prev_launch', 'some_third_file')
        with open(some_file, 'w'):
            pass
        with open(some_other_file, 'w'):
            pass
        with open(some_third_file, 'w'):
            pass

        logger.debug("Test directory tree before running RecoverTask.")
        _log_dir()

        task_spec = {
            'fizzle_on_no_restart_file': False,
            'other_glob_patterns': ['some_file', 'some_*_file'],
        }
        task_spec = dict_merge(self.default_task_spec, task_spec)

        fw_spec = {
            '_job_info': [{
                'launch_dir': 'dummy_prev_launch',
                'name': 'dummy parent',
                'fw_id': 1,
            }]
        }
        fw_spec = dict_merge(self.default_fw_spec, fw_spec)

        logger.debug("Instantiate RecoverTask with '{}'".format(task_spec))
        t = RecoverTask(**task_spec)
        logger.debug("Run with fw_spec '{}'".format(fw_spec))
        fw_action = t.run_task(fw_spec)
        logger.debug("FWAction:")
        _log_nested_dict(fw_action.as_dict())
        logger.debug("Test directory tree after running RecoverTask.")
        _log_dir()

        some_recovered_file = glob.glob('some_file')
        self.assertEqual(len(some_recovered_file), 1)
        self.assertEqual(some_recovered_file[0], "some_file")

        some_other_recovered_files = glob.glob('some_*_file')
        self.assertEqual(len(some_other_recovered_files), 2)
        self.assertIn("some_other_file", some_other_recovered_files)
        self.assertIn("some_third_file", some_other_recovered_files)

    def test_restart_wf_insertion(self):
        """Run a recovery task that returns a restart detour."""
        logger = logging.getLogger(__name__)
        logger.info("### {} ###".format(sys._getframe().f_code.co_name))

        # +-----------------+     +-----------------+     +------------------+
        # | restart_wf root | --> | restart_wf body | --> | restart_wf leafs |
        # +-----------------+     +-----------------+     +------------------+
        dummy_ft = PyTask(func='print', args=['I am a dummy task'])
        restart_root_fw = Firework([dummy_ft], name='restart_wf root')
        restart_body_fw = Firework([dummy_ft], name='restart_wf body', parents=[restart_root_fw])
        restart_leaf_fw = Firework([dummy_ft], name='restart_wf leaf', parents=[restart_body_fw])

        restart_wf = Workflow([restart_root_fw, restart_body_fw, restart_leaf_fw])

        logger.debug("Restart wf:")
        _log_nested_dict(restart_wf.as_dict())

        task_spec = {
            'fizzle_on_no_restart_file': False,
            'repeated_recover_fw_name': 'recover_fw',
            'restart_wf': restart_wf.as_dict(),
        }
        task_spec = dict_merge(self.default_task_spec, task_spec)

        fw_spec = {
            '_job_info': [{
                'launch_dir': 'dummy_prev_launch',
                'name': 'dummy parent',
                'fw_id': 1,
            }]
        }
        fw_spec = dict_merge(self.default_fw_spec, fw_spec)

        logger.debug("Instantiate RecoverTask with '{}'".format(task_spec))
        t = RecoverTask(**task_spec)
        logger.debug("Run with fw_spec '{}'".format(fw_spec))
        fw_action = t.run_task(fw_spec)
        logger.debug("FWAction:")
        _log_nested_dict(fw_action.as_dict())

        detour = fw_action.detours[0]
        self.assertIsInstance(detour, Workflow)

        self.assertEqual(len(detour.leaf_fw_ids), 1)
        self.assertEqual(len(detour.root_fw_ids), 1)

        root_fw_id = detour.root_fw_ids[0]
        self.assertEqual(detour.id_fw[root_fw_id].name, 'restart_wf root')

        self.assertEqual(len(detour.links[root_fw_id]), 1)
        body_fw_id = detour.links[root_fw_id][0]
        self.assertEqual(detour.id_fw[body_fw_id].name, 'restart_wf body')

        self.assertEqual(len(detour.links[body_fw_id]), 1)
        leaf_fw_id = detour.links[body_fw_id][0]
        self.assertEqual(detour.id_fw[leaf_fw_id].name, 'restart_wf leaf')

        self.assertEqual(len(detour.links[leaf_fw_id]), 1)
        recover_fw_id = detour.links[leaf_fw_id][0]
        self.assertEqual(detour.id_fw[recover_fw_id].name, 'recover_fw')

if __name__ == '__main__':
    unittest.main()
