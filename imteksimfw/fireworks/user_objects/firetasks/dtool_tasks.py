# coding: utf-8

from __future__ import unicode_literals

import collections
import datetime
import getpass
import glob
import logging
import io
import json
import os

from ruamel.yaml import YAML

# from dtoolcore import DataSetCreator
import dtoolcore
import dtoolcore.utils
import dtool_create.dataset

from fireworks.core.firework import FiretaskBase, FWAction
from fireworks.utilities.dict_mods import get_nested_dict_value


__author__ = 'Johannes Hoermann'
__copyright__ = 'Copyright 2020, IMTEK Simulation, Univeristy of Freiburg'
__email__ = 'johannes.hoermann@imtek.uni-freiburg.de'
__date__ = 'Apr 27, 2020'


# TODO: add option to retrieve uuid / uri of created / copied datasets
# and insert back into workflow

def _log_nested_dict(log_func, dct):
    for l in json.dumps(dct, indent=2, default=str).splitlines():
        log_func(l)


# from https://gist.github.com/angstwad/bf22d1822c38a92ec0a9
def dict_merge(dct, merge_dct, add_keys=True):
    """ Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
    updating only top-level keys, dict_merge recurses down into dicts nested
    to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
    ``dct``.

    This version will return a copy of the dictionary and leave the original
    arguments untouched.

    The optional argument ``add_keys``, determines whether keys which are
    present in ``merge_dict`` but not ``dct`` should be included in the
    new dict.

    Args:
        dct (dict) onto which the merge is executed
        merge_dct (dict): dct merged into dct
        add_keys (bool): whether to add new keys

    Returns:
        dict: updated dict
    """
    dct = dct.copy()
    if not add_keys:
        merge_dct = {
            k: merge_dct[k]
            for k in set(dct).intersection(set(merge_dct))
        }

    for k, v in merge_dct.items():
        if (k in dct and isinstance(dct[k], dict)
                and isinstance(v, collections.Mapping)):
            dct[k] = dict_merge(dct[k], v, add_keys=add_keys)
        else:
            dct[k] = v

    return dct


# TODO: bug report, there seems to be an issue with
# https://github.com/jic-dtool/dtoolcore/blob/6aff99531d1192f86512f662caf22a6ecd2198a5/dtoolcore/__init__.py#L111
# not respecting custon config paths
def _generate_uri(admin_metadata, base_uri, config_path=None):
    """Return dataset URI.
    :param admin_metadata: dataset administrative metadata
    :param base_uri: base URI from which to derive dataset URI
    :returns: dataset URI
    """
    logger = logging.getLogger(__name__)
    logger.debug("In _generate_uri...")
    logger.debug("_generate_uri.input_base_uri: {}".format(base_uri))
    name = admin_metadata["name"]
    uuid = admin_metadata["uuid"]
    # storage_broker_lookup = _generate_storage_broker_lookup()
    # parse_result = urlparse(base_uri)
    # storage = parse_result.scheme
    StorageBroker = dtoolcore._get_storage_broker(base_uri, config_path)
    uri = StorageBroker.generate_uri(name, uuid, base_uri)
    logger.debug("_generate_uri.return: {}".format(uri))
    return uri



# adapted from https://github.com/jic-dtool/dtool-create/blob/0a772aa5157523a7219963803293d4e521bc1aa2/dtool_create/dataset.py#L40
def _get_readme_template(
        fpath=None, config_path=dtoolcore.utils.DEFAULT_CONFIG_PATH):
    """Allows for spcifying deviating dtool config and readme template file."""

    if fpath is None:
        fpath = dtoolcore.utils.get_config_value(
            "DTOOL_README_TEMPLATE_FPATH",
            config_path
        )
    if fpath is None:
        fpath = dtool_create.dataset.README_TEMPLATE_FPATH

    with open(fpath) as fh:
        readme_template = fh.read()

    user_email = dtoolcore.utils.get_config_value(
        "DTOOL_USER_EMAIL",
        config_path,
        "you@example.com"
    )

    user_full_name = dtoolcore.utils.get_config_value(
        "DTOOL_USER_FULL_NAME",
        config_path,
        "Your Name"
    )

    readme_template = readme_template.format(
        username=getpass.getuser(),
        DTOOL_USER_FULL_NAME=user_full_name,
        DTOOL_USER_EMAIL=user_email,
        date=datetime.date.today(),
    )

    return readme_template


class CreateDatasetTask(FiretaskBase):
    """
    A Firetask to create a dtool data set.

    Required params:
        None
    Optional params:
        - name (str): Name of dataset, default: "dataset".
        - paths (list/str): Either list of paths or a glob pattern string.
            Per default, all content of 'directory'.
        - directory (str): Path to directory where to do pattern matching.
            Per default '.', i.e. current working directory.
        - dtool_config_path (str): Override dtool DEFAULT_CONFIG_PATH imported
            from dtoolcore.utils. Default: None.
        - dtool_readme_template_path (str): Override default dtool readme
            template path. Default: None.
        - metadata_key (str): If not None, then get additional dynamic
            metadata from the specified key within 'specs' and merge with
            README.yml template and static 'metadata' dict.
            Specified key must point to a dict.
            Specify nested fields with a '->' or '.' delimiter.
            If specified key does not exist or is no dict, then ignore.
            Default: 'metadata'
        - metadata (dict): Static metadata to attach to data set.
            Static metadata takes precendence over dynamic metadata in case of
            overlap. Also overrides fields specified in readme template.

    The dataset's README.yml file is a successive merge of the README template,
    static metadata and dynamic metadata, ordered by increasing precedence in
    the case of conflicting fields.
    """
    _fw_name = 'CreateDatasetTask'
    optional_params = [
        "name", "paths", "directory", "metadata", "metadata_key",
        "dtool_readme_template_path", "dtool_config_path"]

    def run_task(self, fw_spec):
        logger = logging.getLogger(__name__)

        name = self.get("name", "dataset")
        # see https://github.com/jic-dtool/dtoolcore/blob/6aff99531d1192f86512f662caf22a6ecd2198a5/dtoolcore/utils.py#L254
        if not dtoolcore.utils.name_is_valid(name):
            raise ValueError((
                "The dataset name can only be 80 characters long. "
                "Valid characters: Alpha numeric characters [0-9a-zA-Z]"
                "Valid special characters: - _ ."))

        logger.info("Create dtool dataset '{}'.".format(name))

        dtool_config_path = self.get(
            "dtool_config_path", dtoolcore.utils.DEFAULT_CONFIG_PATH)
        logger.info("Use dtool config '{}'.".format(dtool_config_path))

        dtool_readme_template_path = self.get(
            "dtool_readme_template_path", None)
        logger.info("Use dtool README.yml template '{}'.".format(
            dtool_readme_template_path))

        dtool_readme_template = _get_readme_template(
            dtool_readme_template_path, dtool_config_path)
        logger.debug("dtool README.yml template content:")
        _log_nested_dict(logger.debug, dtool_readme_template)

        # generate list of files to place tihin dataset
        directory = os.path.abspath(self.get("directory", "."))
        paths = self.get("paths", None)
        abspaths = []
        if paths is not None:
            if isinstance(self["paths"], list):
                logger.info("Treating 'paths' field as list of files.")
                relpaths = paths
                abspaths = [os.path.abspath(p) for p in self["paths"]]
            else:
                logger.info("Treating 'paths' field as glob pattern.")
                logger.info("Searching within '{}'.".format(directory))
                relpaths = list(
                    glob.glob("{}/{}".format(directory, self["paths"])))
                abspaths = [os.path.abspath(p) for p in relpaths]
        else:  # everything within cwd
            logger.info("'paths' not specified,")
            logger.info("Adding all content of '{}'.".format(directory))
            relpaths = []
            abspaths= []
            # just list files, not directories
            for root, subdirs, files in os.walk('.'):
                for f in files:
                    p = os.path.join(root, f)
                    relpaths.append(p)
                    abspaths.append(os.path.abspath(p))

        logger.debug("Items to add to dataset:")
        logger.debug(relpaths)
        logger.debug("Corresponding absolute paths on file system:")
        logger.debug(abspaths)

        # README.yml metadata merging:

        # see https://github.com/jic-dtool/dtool-create/blob/0a772aa5157523a7219963803293d4e521bc1aa2/dtool_create/dataset.py#L244
        yaml = YAML()
        yaml.explicit_start = True
        yaml.indent(mapping=2, sequence=4, offset=2)
        template_metadata = yaml.load(dtool_readme_template)

        logger.debug("Parsed readme template metadata:")
        _log_nested_dict(logger.debug, template_metadata)

        # fw_spec dynamic metadata
        metadata_key = self.get("metadata_key", "metadata")
        try:
            dynamic_metadata = get_nested_dict_value(fw_spec, metadata_key)
        except Exception:  # key not found
            dynamic_metadata = {}

        logger.debug("Dynamic fw_spec metadata:")
        _log_nested_dict(logger.debug, dynamic_metadata)

        # fw_task static metadata
        static_metadata = self.get("metadata", {})
        logger.debug("Static task-level metadata:")
        _log_nested_dict(logger.debug, static_metadata)

        metadata = {}
        metadata = dict_merge(template_metadata, dynamic_metadata)
        metadata = dict_merge(metadata, static_metadata)
        logger.debug("Merged metadata:")
        _log_nested_dict(logger.debug, metadata)

        readme_stream = io.StringIO()
        yaml.dump(metadata, readme_stream)
        readme = readme_stream.getvalue()

        logger.debug("Content of generated README.yml:")
        for l in readme.splitlines():
            logger.debug("  " + l)

        # dtool default owner metadata for dataset creation
        admin_metadata = dtoolcore.generate_admin_metadata(name)
        # looks like
        # >>> admin_metadata = dtoolcore.generate_admin_metadata("test")
        # >>> admin_metadata
        # {
        #   'uuid': 'c19bab1d-8f6b-4e44-99ad-1b09ea198dfe',
        #   'dtoolcore_version': '3.17.0',
        #   'name': 'test',
        #   'type': 'protodataset',
        #   'creator_username': 'jotelha',
        #   'created_at': 1587987156.127988
        # }
        logger.debug("Dataset admin metadata:")
        _log_nested_dict(logger.debug, admin_metadata)

        # see https://github.com/jic-dtool/dtool-create/blob/0a772aa5157523a7219963803293d4e521bc1aa2/dtool_create/dataset.py#L120
        parsed_base_uri = dtoolcore.utils.generous_parse_uri(os.getcwd())
        # create dataset in current working directory

        # NOTE: dtool_create.dataset.create has specific symlink scheme treatment at
        # https://github.com/jic-dtool/dtool-create/blob/0a772aa5157523a7219963803293d4e521bc1aa2/dtool_create/dataset.py#L127
        # not treated here.

        proto_dataset = dtoolcore.generate_proto_dataset(
            admin_metadata=admin_metadata,
            base_uri=dtoolcore.utils.urlunparse(parsed_base_uri),
            config_path=dtool_config_path)
        proto_dataset.create()
        proto_dataset.put_readme(readme)

        # add items to dataset one by one
        # TODO: possibility for per-item metadata
        for abspath, relpath in zip(abspaths, relpaths):
            logger.info("Add '{}' as '{}' to dataset '{}'.".format(
                abspath, relpath, name))
            proto_dataset.put_item(abspath, relpath)

        # freeze dataset
        # see https://github.com/jic-dtool/dtool-create/blob/0a772aa5157523a7219963803293d4e521bc1aa2/dtool_create/dataset.py#L438

        num_items = len(list(proto_dataset._identifiers()))
        logger.info("{} items in dataset '{}'.".format(num_items, name))
        max_files_limit = int(dtoolcore.utils.get_config_value(
            "DTOOL_MAX_FILES_LIMIT",
            dtool_config_path,
            10000
        ))

        assert isinstance(max_files_limit, int)
        if num_items > max_files_limit:
            raise ValueError(
                "Too many items ({} > {}) in proto dataset".format(
                    num_items,
                    max_files_limit
                ))

        handles = [h for h in proto_dataset._storage_broker.iter_item_handles()]
        for h in handles:
            if not dtool_create.utils.valid_handle(h):
                raise ValueError("Invalid item name: {}".format(h))

        logger.debug("Item handles:")
        _log_nested_dict(logger.debug, handles)
        proto_dataset.freeze()

        output = {
            'uri': proto_dataset.uri,
            'uuid': proto_dataset.uuid,
            'name': proto_dataset.name,
        }

        return FWAction(stored_data=output)

# see https://github.com/jic-dtool/dtool-create/blob/0a772aa5157523a7219963803293d4e521bc1aa2/dtool_create/dataset.py#L494
class CopyDatasetTask(FiretaskBase):
    """Copy a dtool data set from source to target.

    Required params:
        - target (str): base URI of target
    Optional params:
        - source (str): URI of source dataset, default: "dataset".
        - resume (bool): continue to copy a dataset existing already partially
            at target.
        - dtool_config_path (str): Override dtool DEFAULT_CONFIG_PATH imported
            from dtoolcore.utils. Default: None.
    """

    _fw_name = 'CopyDatasetTask'
    required_params = ["target"]
    optional_params = [
        "source", "resume", "dtool_config_path"]

    def run_task(self, fw_spec):
        logger = logging.getLogger(__name__)

        source = self.get("source", "dataset")
        target = self.get("target")
        resume = self.get("resume", False)
        dtool_config_path = self.get(
            "dtool_config_path") #, dtoolcore.utils.DEFAULT_CONFIG_PATH)
        logger.info("Use dtool config '{}'.".format(dtool_config_path))

        src_dataset = dtoolcore.DataSet.from_uri(source)

        dest_uri = _generate_uri(
            admin_metadata=src_dataset._admin_metadata,
            base_uri=target, config_path=dtool_config_path
        )
        logger.info("Copy from '{}'".format(source))
        logger.info("  to '{}'.".format(dest_uri))

        if not resume:
            # Check if the destination URI is already a dataset
            # and exit gracefully if true.
            if dtoolcore._is_dataset(dest_uri, config_path=dtool_config_path):
                raise FileExistsError(
                    "Dataset already exists: {}".format(dest_uri))

            # If the destination URI is a "file" dataset one needs to check if
            # the path already exists and exit gracefully if true.
            parsed_dataset_uri = dtoolcore.utils.generous_parse_uri(dest_uri)
            if parsed_dataset_uri.scheme == "file":
                if os.path.exists(parsed_dataset_uri.path):
                    raise FileExistsError(
                        "Path already exists: {}".format(parsed_dataset_uri.path))

            logger.info("Resume.")
            copy_func = dtoolcore.copy
        else:
            copy_func = dtoolcore.copy_resume

        target_uri = copy_func(
            src_uri=source,
            dest_base_uri=target,
            config_path=dtool_config_path
        )
        logger.info("Copied to '{}'.".format(target_uri))

        output = {
            'target_uri': target_uri,
        }
        output = {
            'uri': target_uri,
            'uuid': src_dataset.uuid,  # must be the same
            'name': src_dataset.name,  # must be the same
        }

        return FWAction(stored_data=output)
