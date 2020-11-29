# coding: utf-8
#
# dtool_tasks.py
#
# Copyright (C) 2020 IMTEK Simulation
# Author: Johannes Hoermann, johannes.hoermann@imtek.uni-freiburg.de
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
"""Simple Fireworks interface for handling dtool datasets."""

from abc import abstractmethod
from contextlib import ExitStack

import copy  # for deep copies of nested objects, avoid references in YAML dumps
import datetime
import getpass  # get system username for dtool metadata
import glob
import io
import logging
import os
import shutil
import uuid

from ruamel.yaml import YAML

# from dtoolcore import DataSetCreator
import dtoolcore
import dtoolcore.utils
import dtool_create.dataset

from fireworks.fw_config import FW_LOGGING_FORMAT

from fireworks.core.firework import FWAction
from fireworks.utilities.dict_mods import get_nested_dict_value
from fireworks.utilities.fw_serializers import ENCODING_PARAMS

from imteksimfw.utils.environ import TemporaryOSEnviron
from imteksimfw.utils.dict import simple_dict_merge as dict_merge
from imteksimfw.utils.logging import LoggingContext, _log_nested_dict
from imteksimfw.utils.multiprocessing import RunAsChildProcessTask


__author__ = 'Johannes Laurin Hoermann'
__copyright__ = 'Copyright 2020, IMTEK Simulation, University of Freiburg'
__email__ = 'johannes.hoermann@imtek.uni-freiburg.de, johannes.laurin@gmail.com'
__date__ = 'Apr 27, 2020'

DEFAULT_FORMATTER = logging.Formatter(FW_LOGGING_FORMAT)


def from_fw_spec(param, fw_spec):
    """Expands param['key'] as key within fw_spec.

    If param is dict hand has field 'key', then return value at specified
    position from fw_spec. Otherwise, return 'param' itself.
    """
    if isinstance(param, dict) and 'key' in param:
        ret = get_nested_dict_value(fw_spec, param['key'])
    else:
        ret = param
    return ret


# adapted from https://github.com/jic-dtool/dtool-create/blob/0a772aa5157523a7219963803293d4e521bc1aa2/dtool_create/dataset.py#L40
def _get_readme_template(fpath=None):
    if fpath is None:
        fpath = dtoolcore.utils.get_config_value(
            "DTOOL_README_TEMPLATE_FPATH",
        )
    if fpath is None:
        fpath = dtool_create.dataset.README_TEMPLATE_FPATH

    with open(fpath, mode='r', **ENCODING_PARAMS) as fh:
        readme_template = fh.read()

    user_email = dtoolcore.utils.get_config_value(
        "DTOOL_USER_EMAIL",
        default="you@example.com"
    )

    user_full_name = dtoolcore.utils.get_config_value(
        "DTOOL_USER_FULL_NAME",
        default="Your Name"
    )

    readme_template = readme_template.format(
        username=getpass.getuser(),
        DTOOL_USER_FULL_NAME=user_full_name,
        DTOOL_USER_EMAIL=user_email,
        date=datetime.date.today(),
    )

    return readme_template


class DtoolTask(RunAsChildProcessTask):
    """
    A dtool task ABC.

    Required params:
        None
    Optional params:
        - dtool_config (dict): dtool config key-value pairs, override
            defaults in $HOME/.config/dtool/dtool.json. Default: None
        - dtool_config_key (str): key to dict within fw_spec, override
            defaults in $HOME/.config/dtool/dtool.json and static dtool_config
            task spec. Default: None.
        - output (str): spec key that will be used to pass
            the handled dataset's properties to child fireworks. Default: None
        - dict_mod (str, default: '_set'): how to insert handled dataset's
            properties into output key, see fireworks.utils.dict_mods
        - propagate (bool, default:None): if True, then set the
            FWAction 'propagate' flag and propagate updated fw_spec not only to
            direct children, but to all descendants down to wokflow's leaves.
        - stored_data (bool, default: False): put handled dataset properties
            into FWAction.stored_data
        - store_stdlog (bool, default: False): insert log output into database
        - stdlog_file (str, Default: NameOfTaskClass.log): print log to file
        - loglevel (str, Default: logging.INFO): loglevel for this task
    """
    _fw_name = 'DtoolTask'
    required_params = [*RunAsChildProcessTask.required_params]
    optional_params = [
        *RunAsChildProcessTask.optional_params,
        "dtool_config",
        "dtool_config_key",
        "stored_data",
        "output",
        "dict_mod",
        "propagate",
        "stdlog_file",
        "store_stdlog",
        "loglevel"]

    @abstractmethod
    def _run_task_internal(self, fw_spec) -> dtoolcore.DataSet:
        """Derivatives implement their functionality here."""
        ...

    def _run_task_as_child_process(self, fw_spec, q, e=None):
        """q is a Queue used to return fw_action."""
        stored_data = self.get('stored_data', False)
        output_key = self.get('output', None)
        dict_mod = self.get('dict_mod', '_set')
        propagate = self.get('propagate', False)

        dtool_config = self.get("dtool_config", {})
        dtool_config_key = self.get("dtool_config_key")

        stdlog_file = self.get('stdlog_file', '{}.log'.format(self._fw_name))
        store_stdlog = self.get('store_stdlog', False)

        loglevel = self.get('loglevel', logging.INFO)

        with ExitStack() as stack:

            if store_stdlog:
                stdlog_stream = io.StringIO()
                logh = logging.StreamHandler(stdlog_stream)
                logh.setFormatter(DEFAULT_FORMATTER)
                stack.enter_context(
                    LoggingContext(handler=logh, level=loglevel, close=False))

            # logging to dedicated log file if desired
            if stdlog_file:
                logfh = logging.FileHandler(stdlog_file, mode='a', **ENCODING_PARAMS)
                logfh.setFormatter(DEFAULT_FORMATTER)
                stack.enter_context(
                    LoggingContext(handler=logfh, level=loglevel, close=True))

            logger = logging.getLogger(__name__)

            logger.debug("task spec level dtool config overrides:")
            _log_nested_dict(logger.debug, dtool_config)

            # fw_spec dynamic dtool_config overrides
            dtool_config_update = {}
            if dtool_config_key is not None:
                try:
                    dtool_config_update = get_nested_dict_value(
                        fw_spec, dtool_config_key)
                    logger.debug("fw_spec level dtool config overrides:")
                    _log_nested_dict(logger.debug, dtool_config_update)
                except Exception:  # key not found
                    logger.warning("{} not found within fw_spec, ignored.".format(
                        dtool_config_key))
            dtool_config.update(dtool_config_update)
            logger.debug("effective dtool config overrides:")
            _log_nested_dict(logger.debug, dtool_config)

            stack.enter_context(TemporaryOSEnviron(env=dtool_config))

            dataset = self._run_task_internal(fw_spec)

        output = {
            'uri': dataset.uri,
            'uuid': dataset.uuid,
            'name': dataset.name,
        }

        if store_stdlog:
            stdlog_stream.flush()
            output['stdlog'] = stdlog_stream.getvalue()

        fw_action = FWAction()

        if stored_data:
            fw_action.stored_data = output

        # 'propagate' only development feature for now
        if hasattr(fw_action, 'propagate') and propagate:
            fw_action.propagate = propagate

        if output_key:  # inject into fw_spec
            fw_action.mod_spec = [{dict_mod: {output_key: output}}]

        # return fw_action
        q.put(fw_action)


class CreateDatasetTask(DtoolTask):
    """
    A Firetask to create a dtool data set.

    This task extends the basic DtoolTask parameters by

    Required params:
        None
    Optional params:
        - creator_username (str): Overrides system username if specified.
            Default: None.
        - directory (str): Path to directory where to do pattern matching.
            Per default '.', i.e. current working directory.
        - dtool_readme_template_path (str): Override default dtool readme
            template path. Default: None.
        - metadata_key (str): If not None, then get additional dynamic
            metadata from the specified key within 'specs' and merge with
            README.yml template and static 'metadata' dict.
            Specified key must point to a dict.
            Specify nested fields with a '->' or '.' delimiter.
            If specified key does not exist or is no dict, then ignore.
            Default: 'metadata'.
        - metadata (dict): Static metadata to attach to data set.
            Static metadata takes precendence over dynamic metadata in case of
            overlap. Also overrides fields specified in readme template.
        - name (str): Name of dataset, default is a v1 UUID.
        - paths (list of str / str): Either list of paths or a glob pattern
            string. Per default, all content of 'directory'.
        - source_dataset_uri (list of str / str): A derived dataset will be
            created if  specified. This is only possible if the source dataset
            is accessible. If this is not the case, this task will raise a
            warning and fall back to the standard dataset creation mechanism.
            If multiple source datasets are specified as a list,
            then the dataset will be derived directly from the first list entry
            If the source datasets are accessible, their names ad UUIDs are
            queried and stored along with their URIs in a list of format

                derived_from:  # or other  label set via 'source_dataset_key'
                - uuid: UUID of the first source dataset if available
                  name: name of the first source dataset if available
                  uri:  URI of the first source dataset
                - uuid: UUID of the second source dataset if available
                  name: name of the second source dataset if available
                  uri:  URI of the second source dataset
                - ...

            in the README.yml. UUID and name fields are omitted if datasets
            are not accessible.
        - source_dataset (list of dict [{str:str}] / dict {str:str}): An
            alternative to above's 'source_dataset_uri'. Can directly specify
            all source datasets' names, uris, and uuids in the format

                source_dataset:
                - uuid: UUID of the first source dataset if available
                  name: name of the first source dataset if available
                  uri:  URI of the first source dataset
                - uuid: UUID of the second source dataset if available
                  name: name of the second source dataset if available
                  uri:  URI of the second source dataset
                - ...

            None of the fields is compulsory. If specified datasets
            are not accessible, then warnings will be raised, but specified
            values will be used as defaults for filling references to sources
            as above. If datasets are accessible, but their names and uuids
            won't agree with the specified defaults, warnings are raised
            accordingly and the actual values will override the defaults.
            If both 'source_dataset_uri' and 'source_dataset' are specified,
            then 'source_dataset_uri' is ignored. Default: None.
        - source_dataset_annotation (bool): If True, then also store
            above's 'derived_from' field as JSON annotation. Default: True.
        - source_dataset_key (str): Key of dependency README.yml entry and
            annotation. Default: 'derived_from'.

    The dataset's README.yml file is a successive merge of the README template,
    static metadata and dynamic metadata, ordered by increasing precedence in
    the case of conflicting fields.

    Fields 'creator_username', 'dtool_readme_template_path', 'name',
    'source_dataset_uri', 'source_dataset', and 'source_dataset_annotation'
    may also be a dict of format { 'key': 'some->nested->fw_spec->key' } for
    looking up value within 'fw_spec' instead.
    """
    _fw_name = 'CreateDatasetTask'
    required_params = [
        *DtoolTask.required_params]

    optional_params = [
        *DtoolTask.optional_params,
        "creator_username",
        "directory",
        "dtool_readme_template_path",
        "metadata",
        "metadata_key",
        "name",
        "paths",
        "source_dataset_uri",
        "source_dataset",
        "source_dataset_annotation",
        "source_dataset_key"]

    def _run_task_internal(self, fw_spec):
        logger = logging.getLogger(__name__)

        name = self.get(
            "name", str(uuid.uuid1()))
        name = from_fw_spec(name, fw_spec)

        # see https://github.com/jic-dtool/dtoolcore/blob/6aff99531d1192f86512f662caf22a6ecd2198a5/dtoolcore/utils.py#L254
        if not dtoolcore.utils.name_is_valid(name):
            raise ValueError((
                "The dataset name can only be 80 characters long. "
                "Valid characters: Alpha numeric characters [0-9a-zA-Z]"
                "Valid special characters: - _ ."))
        logger.info("Create dtool dataset '{}'.".format(name))

        creator_username = self.get(
            "creator_username", None)
        creator_username = from_fw_spec(creator_username, fw_spec)
        if creator_username is not None:
            logger.info("Overriding system username with '{}'.".format(
                creator_username))

        source_dataset_uri = self.get(
            "source_dataset_uri", None)
        source_dataset_uri = from_fw_spec(source_dataset_uri, fw_spec)
        # make sure source_dataset_uri is either str or list of str:
        if isinstance(source_dataset_uri, str):
            source_dataset_uri = [source_dataset_uri]
        if source_dataset_uri is not None:
            try:
                iter(source_dataset_uri)
            except TypeError as exc:
                logger.error("'source_dataset_uri' must be str or list of str.")
                raise exc
            logger.info("Derive from 'source_dataset_uri': '{}'.".format(source_dataset_uri))

        source_dataset = self.get(
            "source_dataset", None)
        source_dataset = from_fw_spec(source_dataset, fw_spec)
        # make sure source_dataset_uri is either dict or list of dict:
        if isinstance(source_dataset, dict):
            source_dataset = [source_dataset]
        if source_dataset is not None:
            try:
                iter(source_dataset)
            except TypeError as exc:
                logger.error("'source_dataset' must be dict or list of dict.")
                raise exc
            logger.info("Derive from 'source_dataset': '{}'.".format(source_dataset))

        if (source_dataset_uri is not None) and (source_dataset is not None):
            logger.info(
                "Both 'source_dataset_uri' and 'source_dataset' are set."
                "Latter will override former.")
        elif source_dataset_uri is not None:  # and not source_dataset
            source_dataset = [{'uri': uri} for uri in source_dataset_uri]

        # from here on, only use source_dataset scheme

        # fill missing fields in source_dataset if uri is present and accessible
        if source_dataset is not None:
            for i, d in enumerate(source_dataset):
                if 'uri' in d:
                    # check whether source dataset accessible
                    try:
                        current_source_dataset = dtoolcore.DataSet.from_uri(
                            d['uri'])
                    except dtoolcore.DtoolCoreTypeError as exc:
                        logger.warning(
                            "Source dataset #{} with URI '{}' not accessible [{}]."
                            .format(i, d['uri'], exc))
                    else:
                        logger.info(
                            "Source dataset #{} has name '{}', uri '{}', and uuid '{}'."
                            .format(i,
                                    current_source_dataset.name,
                                    current_source_dataset.uri,
                                    current_source_dataset.uuid))

                        if 'name' in d and d['name'] != current_source_dataset.name:
                            logger.waning((
                                "Source dataset #{} actual name '{}' "
                                "does not agree with specified default '{}'. "
                                "Former will override latter.").format(
                                    i, current_source_dataset.name, d["name"]))
                        d["name"] = current_source_dataset.name

                        if 'uuid' in d and d['uuid'] != current_source_dataset.uuid:
                            logger.waning((
                                "Source dataset #{} actual UUID '{}' "
                                "does not agree with specified default '{}'. "
                                "Former will override latter.").format(
                                    i, current_source_dataset.uuid, d["uuid"]))
                        d["uuid"] = current_source_dataset.uuid
                else:
                    logger.warning("Source dataset '{}' has no URI specified."
                                .format(d))

        source_dataset_annotation = self.get(
            "source_dataset_annotation", True)
        source_dataset_annotation = from_fw_spec(source_dataset_annotation, fw_spec)

        source_dataset_key = self.get(
            "source_dataset_key", "derived_from")
        source_dataset_key = from_fw_spec(source_dataset_key, fw_spec)

        dtool_readme_template_path = self.get(
            "dtool_readme_template_path", None)
        dtool_readme_template_path = from_fw_spec(dtool_readme_template_path, fw_spec)
        logger.info("Use dtool README.yml template '{}'.".format(
            dtool_readme_template_path))

        dtool_readme_template = _get_readme_template(
            dtool_readme_template_path)
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
            abspaths = []
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

        # source_dataset will override anything else in designated dependency
        # field if specified:
        dependency_metadata = {}
        if source_dataset is not None:
            dependency_metadata = {source_dataset_key: copy.deepcopy(source_dataset)}
            logger.debug("Dependency metadata:")
            _log_nested_dict(logger.debug, dependency_metadata)
        # without deepcopy, references in the dumped structure might appear as
        #   derived_from:
        #     - *id001
        #  in the YAML output instead of the desired
        #   derived_from:  # or other  label set via 'source_dataset_key'
        #     - uuid: UUID of the first source dataset if available
        #       name: name of the first source dataset if available
        #       uri:  URI of the first source dataset

        metadata = {}
        metadata = dict_merge(template_metadata, dynamic_metadata)
        metadata = dict_merge(metadata, static_metadata)
        metadata = dict_merge(metadata, dependency_metadata)
        logger.debug("Merged metadata:")
        _log_nested_dict(logger.debug, metadata)

        readme_stream = io.StringIO()
        yaml.dump(metadata, readme_stream)
        readme = readme_stream.getvalue()

        logger.debug("Content of generated README.yml:")
        for l in readme.splitlines():
            logger.debug("  " + l)

        # dtool default owner metadata for dataset creation
        # admin_metadata = dtoolcore.generate_admin_metadata(name)
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
        # logger.debug("Dataset admin metadata:")
        # _log_nested_dict(logger.debug, admin_metadata)

        # see https://github.com/jic-dtool/dtool-create/blob/0a772aa5157523a7219963803293d4e521bc1aa2/dtool_create/dataset.py#L120
        parsed_base_uri = dtoolcore.utils.generous_parse_uri(os.getcwd())
        # create dataset in current working directory

        # NOTE: dtool_create.dataset.create has specific symlink scheme treatment at
        # https://github.com/jic-dtool/dtool-create/blob/0a772aa5157523a7219963803293d4e521bc1aa2/dtool_create/dataset.py#L127
        # not treated here.

        first_source_dataset = None
        if source_dataset is not None:
            logger.info("Select first accessible source dataset.")
            # find first source dataset with valid URI
            for i, d in enumerate(source_dataset):
                if 'uri' in d:
                    # parsed_source_dataset_uri = dtoolcore.utils.generous_parse_uri(d['uri'])
                    # logger.info("Derive new dataset from '{}'.".format(
                    #     parsed_source_dataset_uri))
                    try:
                        first_source_dataset = dtoolcore.DataSet.from_uri(d["uri"])
                    except dtoolcore.DtoolCoreTypeError as exc:
                        logger.warning("Source dataset #{} with URI '{}' not accessible ( ), skipped.".format(
                                       i, d['uri'], exc))
                    else:
                        logger.info("Found source dataset #{} with URI '{}' accessible.".format(i, d['uri']))
                        logger.info(
                            "Source dataset has name '{}', uri '{}', and uuid '{}'."
                            .format(first_source_dataset.name, first_source_dataset.uri, first_source_dataset.uuid))
                        break
                else:
                    logger.warning("Source dataset #{} has no URI field, skipped.".format(i))
            if first_source_dataset is None:
                logger.warning((
                    "None of specified source datasets accessible. "
                    "Falling back to default dataset creator."))

        if first_source_dataset is not None:
            proto_dataset = dtoolcore.create_derived_proto_dataset(
                name=name,
                base_uri=dtoolcore.utils.urlunparse(parsed_base_uri),
                source_dataset=first_source_dataset,
                readme_content=readme,
                creator_username=creator_username)
        else:   # fall back to default mechanism without source dataset instead
            proto_dataset = dtoolcore.create_proto_dataset(
                name=name,
                base_uri=dtoolcore.utils.urlunparse(parsed_base_uri),
                readme_content=readme,
                creator_username=creator_username)

        # store dependencies as annotations if desired
        if source_dataset_annotation and source_dataset is not None:
            proto_dataset.put_annotation(source_dataset_key, source_dataset)

        # add items to dataset one by one
        # TODO: possibility for per-item metadata
        for abspath, relpath in zip(abspaths, relpaths):
            logger.info("Add '{}' as '{}' to dataset '{}'.".format(
                abspath, relpath, proto_dataset.name))
            proto_dataset.put_item(abspath, relpath)

        logger.info(
            "Created new dataset with name '{}', uri '{}', and uuid '{}'."
            .format(proto_dataset.name, proto_dataset.uri, proto_dataset.uuid))

        return proto_dataset


class FreezeDatasetTask(DtoolTask):
    """
    A Firetask to freeze a dtool data set.

    This task extends the basic DtoolTask parameters by

    Required params:
        None
    Optional params:
        - uri (str): URI of dataset, default: "dataset"

    Field 'uri' may also be a dict of format
    { 'key': 'some->nested->fw_spec->key' } for looking up value within
    fw_spec instead.
    """
    _fw_name = 'FreezeDatasetTask'
    required_params = [*DtoolTask.required_params]
    optional_params = [
        *DtoolTask.optional_params,
        "uri"]

    def _run_task_internal(self, fw_spec):
        logger = logging.getLogger(__name__)

        uri = self.get("uri", "dataset")
        uri = from_fw_spec(uri, fw_spec)

        proto_dataset = dtoolcore.ProtoDataSet.from_uri(uri)
        logger.info("Freezing dataset '{}' with URI '{}'.".format(
            proto_dataset.name, proto_dataset.uri))

        # freeze dataset
        # see https://github.com/jic-dtool/dtool-create/blob/0a772aa5157523a7219963803293d4e521bc1aa2/dtool_create/dataset.py#L438

        num_items = len(list(proto_dataset._identifiers()))
        logger.info("{} items in dataset '{}'.".format(num_items, proto_dataset.name))
        max_files_limit = int(dtoolcore.utils.get_config_value(
            "DTOOL_MAX_FILES_LIMIT",
            default=10000
        ))

        assert isinstance(max_files_limit, int)
        if num_items > max_files_limit:
            raise ValueError(
                "Too many items ({} > {}) in proto dataset".format(
                    num_items,
                    max_files_limit
                ))

        handles = [
            h for h in proto_dataset._storage_broker.iter_item_handles()]
        for h in handles:
            if not dtool_create.utils.valid_handle(h):
                raise ValueError("Invalid item name: {}".format(h))

        logger.debug("Item handles:")
        _log_nested_dict(logger.debug, handles)
        proto_dataset.freeze()

        return proto_dataset


# see https://github.com/jic-dtool/dtool-create/blob/0a772aa5157523a7219963803293d4e521bc1aa2/dtool_create/dataset.py#L494
class CopyDatasetTask(DtoolTask):
    """Copy a dtool data set from source to target.

    This task extends the basic DtoolTask parameters by

    Required params:
        - target (str): base URI of target
    Optional params:
        - source (str): URI of source dataset, default: "dataset".
        - resume (bool): continue to copy a dataset existing already partially
            at target.

    Fields 'target', 'source', and 'resume' may also be a dict of format
    { 'key': 'some->nested->fw_spec->key' } for looking up value within
    fw_spec instead.
    """

    _fw_name = 'CopyDatasetTask'
    required_params = [
        *DtoolTask.required_params,
        "target"]
    optional_params = [
        *DtoolTask.optional_params,
        "source",
        "resume"]

    def _run_task_internal(self, fw_spec):
        logger = logging.getLogger(__name__)

        source = self.get("source", "dataset")
        source = from_fw_spec(source, fw_spec)

        target = self.get("target")
        target = from_fw_spec(target, fw_spec)

        resume = self.get("resume", False)
        resume = from_fw_spec(resume, fw_spec)


        src_dataset = dtoolcore.DataSet.from_uri(source)

        dest_uri = dtoolcore._generate_uri(
            admin_metadata=src_dataset._admin_metadata,
            base_uri=target
        )
        logger.info("Copy from '{}'".format(source))
        logger.info("  to '{}'.".format(dest_uri))

        if not resume:
            # Check if the destination URI is already a dataset
            # and exit gracefully if true.
            if dtoolcore._is_dataset(dest_uri, config_path=None):
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
        )
        logger.info("Copied to '{}'.".format(target_uri))

        target_dataset = dtoolcore.DataSet.from_uri(target_uri)

        return target_dataset


class FetchItemTask(DtoolTask):
    """Fetch a specific item from source dataset to local path.

    This task extends the basic DtoolTask parameters by

    Required params:
        - item_id (str): id of desired item

    Optional params:
        - source (str): URI of source dataset, default: "dataset".
        - dest_dir (str): local destination directory of item. Default: current directory.
        - filename (str)L local filename of item. Default: as returned by dtool

    Fields 'item_id', 'source', 'dest_dir', 'filename' may also be a dict of format
    { 'key': 'some->nested->fw_spec->key' } for looking up value within
    fw_spec instead.
    """

    _fw_name = 'FetchItemTask'
    required_params = [
        *DtoolTask.required_params,
        "item_id"]
    optional_params = [
        *DtoolTask.optional_params,
        "source",
        "dest_dir",
        "filename"]

    def _run_task_internal(self, fw_spec):
        logger = logging.getLogger(__name__)

        item_id = self.get("item_id", None)
        item_id = from_fw_spec(item_id, fw_spec)

        source = self.get("source", "dataset")
        source = from_fw_spec(source, fw_spec)

        dest_dir = self.get("dest_dir", os.path.abspath(os.path.curdir))
        dest_dir = from_fw_spec(dest_dir, fw_spec)

        filename = self.get("filename", None)
        filename = from_fw_spec(filename, fw_spec)

        src_dataset = dtoolcore.DataSet.from_uri(source)
        local_path = src_dataset.item_content_abspath(item_id)

        logger.debug("Local path of '%s' in '%s': %s" % (item_id, source, local_path))

        if filename is None:
            filename = os.path.basename(local_path)
            logger.debug("No explicit filename specified, using '%s'." % filename)

        dest = os.path.join(dest_dir, filename)

        logger.info("Copy from '%s' to '%s'" % (local_path, dest))
        shutil.copy(local_path, dest)

        return src_dataset