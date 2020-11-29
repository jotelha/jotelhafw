# coding: utf-8
"""Fixtures."""
__author__ = 'Johannes Laurin Hoermann'
__copyright__ = 'Copyright 2020, IMTEK Simulation, University of Freiburg'
__email__ = 'johannes.hoermann@imtek.uni-freiburg.de, johannes.laurin@gmail.com'
__date__ = 'Nov 6, 2020'

import json
import logging
import os
import pytest
import tempfile

from imteksimfw.utils.logging import _log_nested_dict

module_dir = os.path.abspath(os.path.dirname(__file__))


def _read_json(file):
    with open(file, 'r') as stream:
        return json.load(stream)


@pytest.fixture
def files():
    """Provide paths to dtool tasks tests files."""
    return {
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


@pytest.fixture
def dtool_config(files):
    """Provide default dtool config."""
    logger = logging.getLogger(__name__)
    # use environment variables instead of custom config file, see
    # https://github.com/jic-dtool/dtoolcore/pull/17
    # _original_environ = os.environ.copy()

    # inject configuration into environment:
    dtool_config = _read_json(files['dtool_config_path'])
    logger.debug("dtool config overrides:")
    _log_nested_dict(logger.debug, dtool_config)

    return dtool_config


@pytest.fixture
def tempdir(request):
    """Provide clean temporary directory."""
    tmpdir = tempfile.TemporaryDirectory()
    previous_working_directory = os.getcwd()
    os.chdir(tmpdir.name)

    def finalizer():
        os.chdir(previous_working_directory)
        tmpdir.cleanup()

    request.addfinalizer(finalizer)
    return
