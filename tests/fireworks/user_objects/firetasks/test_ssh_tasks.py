# coding: utf-8
"""Test dtool integration.

To see verbose logging during testing, run something like

    import logging
    import unittest
    from imteksimfw.fireworks.user_objects.firetasks.tests.test_dtool_tasks import DtoolTasksTest
    logging.basicConfig(level=logging.DEBUG)
    suite = unittest.TestSuite()
    suite.addTest(DtoolTasksTest('name_of_the_desired_test'))
    runner = unittest.TextTestRunner()
    runner.run(suite)
"""
__author__ = 'Johannes Laurin Hoermann'
__copyright__ = 'Copyright 2020, IMTEK Simulation, University of Freiburg'
__email__ = 'johannes.hoermann@imtek.uni-freiburg.de, johannes.laurin@gmail.com'
__date__ = 'May 7, 2020'

import logging
import unittest
import os
import tempfile
import threading
import time
import mockssh

# needs dtool cli for verification

from imteksimfw.fireworks.user_objects.firetasks.ssh_tasks import SSHForwardTask

module_dir = os.path.abspath(os.path.dirname(__file__))


class SSHTasksTest(unittest.TestCase):
    def setUp(self):
        # logger = logging.getLogger(__name__)

        self._tmpdir = tempfile.TemporaryDirectory()
        self._previous_working_directory = os.getcwd()
        os.chdir(self._tmpdir.name)

        self.default_ssh_forward_task_spec = {
            'remote_host': 'localhost',
            'remote_port': 80,
            'ssh_host': 'localhost',
            'ssh_user': 'testuser',
            'local_port': None,  # automatic allocation
            'port_file': '.port',
            # 'ssh_port': 22,
            'ssh_keyfile': os.path.join(module_dir, "id_rsa"),
        }

        self.default_ssh_user = {'testuser': os.path.join(module_dir, "id_rsa")}

    def tearDown(self):
        os.chdir(self._previous_working_directory)
        self._tmpdir.cleanup()

    def test_ssh_forward_task_run(self):
        """Establish ssh forwarding via mock ssh server."""
        logger = logging.getLogger(__name__)

        with mockssh.Server(self.default_ssh_user) as s:
            ssh_port = s.port
            logger.info("Started mock ssh server for users '{}' at port {}."
                .format(s.users, ssh_port))
            logger.debug("Instantiate SSHForwardTask with '{}'".format(
                {**self.default_ssh_forward_task_spec, 'ssh_port': ssh_port}))
            t = SSHForwardTask(ssh_port=ssh_port, **self.default_ssh_forward_task_spec)
            e = threading.Event()
            t.set_stop_event(e)
            thread = threading.Thread(target=t.run_task, kwargs={'fw_spec': {}})
            thread.start()
            logger.info("Started thread '{}.'".format(thread.name))
            time.sleep(5)
            logger.info("Send stop signal.")
            e.set()
            thread.join()
            logger.info("Thread ended.")


if __name__ == '__main__':
    unittest.main()
