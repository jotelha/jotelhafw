# coding: utf-8
#
# dtool_lookup_tasks.py
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
"""Simple dtool-lookup-client interface for querying a dtool lookup server."""
from typing import Dict, List

from abc import abstractmethod
from contextlib import ExitStack

import io
import json
import logging
import os
import pymongo

from ruamel.yaml import YAML

from fireworks.fw_config import FW_LOGGING_FORMAT

from fireworks.core.firework import FWAction
from fireworks.utilities.dict_mods import get_nested_dict_value
from fireworks.utilities.fw_serializers import ENCODING_PARAMS

from imteksimfw.utils.multiprocessing import RunAsChildProcessTask
from imteksimfw.utils.dict import dict_merge
from imteksimfw.utils.environ import TemporaryOSEnviron
from imteksimfw.utils.logging import LoggingContext, _log_nested_dict


__author__ = 'Johannes Laurin Hoermann'
__copyright__ = 'Copyright 2020, IMTEK Simulation, University of Freiburg'
__email__ = 'johannes.hoermann@imtek.uni-freiburg.de, johannes.laurin@gmail.com'
__date__ = 'Nov 03, 2020'


# TODO: dates might be string or float, fix on server side
Date = (str, float)

DEFAULT_FORMATTER = logging.Formatter(FW_LOGGING_FORMAT)

# fields expected in dtool-lookup-server's response
FIELD_TYPE_DICT = {
    'base_uri': str,
    'created_at': Date,
    'creator_username': str,
    'frozen_at': Date,
    'name': str,
    'uri': str,
    'uuid': str,
}

yaml = YAML()


def write_serialized(obj, file, format=None):
    """Write serializable object to file. Format from file suffix."""
    if not format:
        _, extension = os.path.splitext(file)
        format = extension[1:].strip().lower()

    with open(file, "w") as f:
        if format in ['json']:
            json.dump(obj, f)
        else:
            yaml.dump(obj, f)


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


# TODO: this should rather go into dtool-lookup-client itself
def validate_dataset_info(d):
    """Check all expected fields exist."""
    for f, t in FIELD_TYPE_DICT.items():
        if f not in d:
            raise KeyError("Expected key '{}' not in '{}'".format(f, d))

        if not isinstance(d[f], t):
            raise KeyError(
                "Key '{}' value type('{}') is '{}', but '{}' expected.".format(
                    f, d[f], type(d[f]), t))


# TODO: This follows almost the same pattern as DtoolTask, further abstraction possible
class DtoolLookupTask(RunAsChildProcessTask):
    """
    A dtool lookup task ABC.

    Required params:
        None
    Optional params:
        - dtool_config (dict): dtool config key-value pairs, override
            defaults in $HOME/.config/dtool/dtool.json. Default: None
        - dtool_config_key (str): key to dict within fw_spec, override
            defaults in $HOME/.config/dtool/dtool.json and static dtool_config
            task spec. Default: None.
        - output (str): spec key that will be used to pass
            the query result's content to child fireworks. Default: None
        - dict_mod (str, default: '_set'): how to insert handled dataset's
            properties into output key, see fireworks.utils.dict_mods
        - propagate (bool, default:None): if True, then set the
            FWAction 'propagate' flag and propagate updated fw_spec not only to
            direct children, but to all descendants down to wokflow's leaves.
        - stored_data (bool, default: False): put handled dataset properties
            into FWAction.stored_data
        - store_stdlog (bool, default: False): insert log output into FWAction.stored_data
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
    def _run_task_internal(self, fw_spec) -> List[Dict]:
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

            output = self._run_task_internal(fw_spec)

        fw_action = FWAction()

        if stored_data or store_stdlog:
            fw_action.stored_data = {}
        if store_stdlog:
            stdlog_stream.flush()
            fw_action.stored_data['stdlog'] = stdlog_stream.getvalue()
        if stored_data:
            fw_action.stored_data['output'] = output

        # 'propagate' only development feature for now
        if hasattr(fw_action, 'propagate') and propagate:
            fw_action.propagate = propagate

        if output_key:  # inject into fw_spec
            fw_action.mod_spec = [{dict_mod: {output_key: output}}]

        # return fw_action
        q.put(fw_action)


class QueryDtoolTask(DtoolLookupTask):
    """
    A Firetask to query datasets from via dtool lookup server and write the
    them to specified directory (current working directory if not specified).

    Required params:

    Optional params:
        - query (dict): mongo db query identifying files to fetch.
          Same as within fireworks.utils.dict_mods, use '->' in dict keys
          for querying nested documents, instead of MongoDB '.' (dot) seperator.
          Do use '.' and NOT '->' within the 'sort_key' field.
        - query_key (str): entry in fw_spec this key points to supersedes 'query' if specified
        - sort_key (str): sort key, sort by 'frozen_at' per default
        - sort_direction (int): sort direction, default 'pymongo.DESCENDING'
        - limit (int): maximum number of files to write, default: no limit
        - fizzle_empty_result (bool): fizzle if nothing found, default: True
        - fizzle_degenerate_dataset_name (bool): fizzle if more than one of the
          resulting datasets are named equivalently, default: False
        - metadata_file (str): default: None. If specififed, then metadata of
          queried files is written in .yaml or .json format, depending on the
          file name suffix.
        - expand (bool): will replace list result with single value result if
          list result only contains one entry and with None if list result is empty.
          Default: False.

    Fields 'sort_key', 'sort_direction', 'limit', 'fizzle_empty_result',
    'fizzle_degenerate_dataset_name', 'meta_file', 'expand', may also be a dict of format
    { 'key': 'some->nested->fw_spec->key' } for looking up value within
    fw_spec instead.
    """

    _fw_name = 'QueryDatasetTask'
    required_params = [
        *DtoolLookupTask.required_params,
    ]
    optional_params = [
        *DtoolLookupTask.optional_params,
        "query", "query_key",
        "fizzle_degenerate_dataset_name", "fizzle_empty_result",
        "limit", "metadata_file",
        "sort_direction", "sort_key", "expand"]

    def _run_task_internal(self, fw_spec):
        import dtool_lookup_api
        dtool_lookup_api.core.config.Config.interactive = False

        logger = logging.getLogger(__name__)

        query = self.get("query", {})
        query_key = self.get("query_key", None)

        sort_key = self.get("sort_key", 'frozen_at')
        sort_key = from_fw_spec(sort_key, fw_spec)
        if sort_key not in FIELD_TYPE_DICT:
            raise ValueError(
                "'sort_key' is '{}', but must be selected from '{}'".format(
                    sort_key, list(FIELD_TYPE_DICT.keys())
                ))

        sort_direction = self.get("sort_direction", pymongo.DESCENDING)
        sort_direction = from_fw_spec(sort_direction, fw_spec)
        if sort_direction not in [pymongo.ASCENDING, pymongo.DESCENDING]:
            raise ValueError(
                "'sort_direction' is '{}', but must be selected from '{}'".format(
                    sort_key, [pymongo.ASCENDING, pymongo.DESCENDING]
                ))

        limit = self.get("limit", None)
        limit = from_fw_spec(limit, fw_spec)

        fizzle_empty_result = self.get("fizzle_empty_result", True)
        fizzle_empty_result = from_fw_spec(fizzle_empty_result, fw_spec)

        fizzle_degenerate_dataset_name = self.get("fizzle_degenerate_dataset_name", True)

        meta_file = self.get("meta_file", None)
        meta_file = from_fw_spec(meta_file, fw_spec)

        expand = self.get('expand', None)
        expand = from_fw_spec(expand, fw_spec)

        if query_key:
            logger.debug("query from fw_spec at '%s'." % query_key)
            query = get_nested_dict_value(fw_spec, query_key)
        elif input:
            pass
        else:
            raise ValueError("Neither 'query' nor 'query_key' specified.")

        r = dtool_lookup_api.query(query)

        logger.debug("Server response: '{}'".format(r))

        if fizzle_empty_result and (len(r) == 0):
            raise ValueError("Query yielded empty result! (query: {})".format(
                query))

        for doc in r:
            logger.debug("Validating '{}'...".format(doc))
            validate_dataset_info(doc)

        if sort_key:
            logger.debug("Sorting by key '{}' and direction '{}'.".format(
                         sort_key, sort_direction))
            r = sorted(r, key=lambda d: d[sort_key],
                       reverse=(sort_direction == pymongo.DESCENDING))
            logger.debug("Sorted server response: '{}'".format(r))

        if limit:
            logger.debug("Limiting response to the first {} enrties...".format(
                         limit))
            r = r[:limit]
            logger.debug("Truncated server response: '{}'".format(r))

        unique_dataset_names = set()  # track all used file names

        for i, doc in enumerate(r):
            dataset_name = doc["name"]
            if fizzle_degenerate_dataset_name and (dataset_name in unique_dataset_names):
                raise ValueError((
                    "The dataset name {} is used "
                    "a second time by result {:d}/{:d}! (query: {})").format(
                        dataset_name, i, len(r), query))

            unique_dataset_names.add(dataset_name)

        if expand and len(r) == 1:
            r = r[0]
            logger.debug("Expand single-entry result'%s'." % r)
        elif expand and len(r) == 0:
            r = None
            logger.debug("Expand empty result as None.")

        if meta_file:
            logger.debug("Write response to file '{}'.".format(meta_file))
            write_serialized(r, meta_file)

        return r


class ReadmeDtoolTask(DtoolLookupTask):
    """
    Firetask to query a dataset's README via dtool lookup server and inject
    content into fw_spec.

    Required params:
        - uri (str): dataset URI

    Optional params:
        - dest_dir (str): destination directory, default is the current working
            directory.
        - fizzle_empty_result (bool): fizzle if nothing found, default: True
        - fw_supersedes_dtool: if True, metadata from metadata_fw_source_key
            supersedes metadata from metadata_fp_source_key. Otherwise, latter
            supersedes former. Default: False
        - metadata_dtool_exclusions (dict): mark exclusions from metadata forwarded
            into pipeline with nested dict {'excluded': {'field': True}}.
        - metadata_dtool_source_key (str): if specified, only use nested subset of
            metadata found in README. Default: None or "" uses all.
        - metadata_file (str): default: None. If specififed, then metadata of
            queried files is written in .yaml or .json format, depending on the
            file name suffix.
        - metadata_fw_exclusions (dict): mark exclusions from
            metadata_fw_source_key merged with metadata_fp_source_key.
        - metadata_fw_source_key (str): merge metadata from README with
            metadata within this fw_spec key before forwarding.

    Fields 'uri', 'sort_direction', 'dest_dir', 'fizzle_empty_result',
    'fw_supersedes_dtool', 'metadata_dtool_source_key', 'metadata_file',
    'metadata_fw_source_key' may also be a dict of format
    { 'key': 'some->nested->fw_spec->key' } for looking up value within
    fw_spec instead.
    """
    _fw_name = 'ReadmeDatasetTask'
    required_params = [
        *DtoolLookupTask.required_params,
        "uri"]
    optional_params = [
        *DtoolLookupTask.optional_params,
        "fizzle_empty_result",
        "metadata_file",

        "metadata_dtool_source_key",
        "metadata_fw_source_key",
        "fw_supersedes_dtool",
        "metadata_dtool_exclusions",
        "metadata_fw_exclusions",
    ]

    def _run_task_internal(self, fw_spec):
        import dtool_lookup_api
        dtool_lookup_api.core.config.Config.interactive = False

        logger = logging.getLogger(__name__)

        uri = self.get("uri", None)
        uri = from_fw_spec(uri, fw_spec)

        dest_dir = self.get("dest_dir", os.path.abspath(os.path.curdir))
        dest_dir = from_fw_spec(dest_dir, fw_spec)

        fizzle_empty_result = self.get("fizzle_empty_result", True)
        fizzle_empty_result = from_fw_spec(fizzle_empty_result, fw_spec)

        metadata_file = self.get("metadata_file", None)
        metadata_file = from_fw_spec(metadata_file, fw_spec)

        metadata_dtool_source_key = self.get("metadata_dtool_source_key", None)
        metadata_dtool_source_key = from_fw_spec(metadata_dtool_source_key, fw_spec)

        metadata_fw_source_key = self.get("metadata_fw_source_key", None)
        metadata_fw_source_key = from_fw_spec(metadata_fw_source_key, fw_spec)

        fw_supersedes_dtool = self.get("fw_supersedes_dtool", False)
        fw_supersedes_dtool = from_fw_spec(fw_supersedes_dtool, fw_spec)

        metadata_dtool_exclusions = self.get("metadata_dtool_exclusions", {})
        metadata_fw_exclusions = self.get("metadata_fw_exclusions", {})

        readme = dtool_lookup_api.readme(uri)
        metadata = {}
        if isinstance(readme, str) and fizzle_empty_result:  # server error message
            raise ValueError(readme)
        elif isinstance(readme, str):
            logger.warning(readme)
        else:
            logger.debug("Metadata of queried object:")
            _log_nested_dict(logger.debug, readme)

            if metadata_file:
                metadata_file_path = os.path.join(dest_dir, metadata_file)
                logger.debug("Write metadata of queried dataset to file '{}'.".format(metadata_file_path))
                write_serialized(readme, metadata_file_path)

            if metadata_dtool_source_key and metadata_dtool_source_key != "":
                dtool_metadata = get_nested_dict_value(readme, metadata_dtool_source_key)
                logger.debug(
                    "Forwarding '{}' subset of README metadata:".format(metadata_dtool_source_key))
                _log_nested_dict(logger.debug, dtool_metadata)
            else:
                dtool_metadata = readme
                logger.debug("Forwarding README metadata.")

            if metadata_fw_source_key and metadata_fw_source_key == "":
                fw_metadata = fw_spec
                logger.debug("Merging with fw_spec.")
            elif metadata_fw_source_key:
                fw_metadata = get_nested_dict_value(fw_spec, metadata_fw_source_key)
                logger.debug("Merging with '{}' subset of fw_spec:".format(metadata_fw_source_key))
                _log_nested_dict(logger.debug, fw_metadata)
            else:
                fw_metadata = {}

            if fw_supersedes_dtool:
                metadata = dict_merge(dtool_metadata, fw_metadata,
                                      exclusions=metadata_dtool_exclusions,
                                      merge_exclusions=metadata_fw_exclusions)
            else:
                metadata = dict_merge(fw_metadata, dtool_metadata,
                                      exclusions=metadata_fw_exclusions,
                                      merge_exclusions=metadata_dtool_exclusions)

            logger.debug("Forwarding merge:")
            _log_nested_dict(logger.debug, metadata)

        return metadata


class ManifestDtoolTask(DtoolLookupTask):
    """
    Firetask to query a dataset's manifest via dtool lookup server. A manifest may look like this:

        {
            'dtoolcore_version': '3.17.0',
            'hash_function': 'md5sum_hexdigest',
            'items': {
                '0a03867efe1eb36d257d4cecf598019963a1f14d': {
                    'hash': '442170a2fd3fa9a54a711ffa1b249e43',
                    'relpath': 'surfactant_head_surfactant_head_rdf.txt',
                    'size_in_bytes': 115098,
                    'utc_timestamp': 1600729024.880694},
                '0b6c156fb7e71a1387904cc6d918cb9acf286538': {
                    'hash': 'bf9cb747ae826511958790034e62d773',
                    'relpath': 'counterion_counterion_rdf.txt',
                    'size_in_bytes': 115098,
                    'utc_timestamp': 1600729024.979725
                },
                ...
            }
        }

    Required params:
        - uri (str): dataset URI

    Optional params:
        - dest_dir (str): destination directory, default is the current working
            directory.
        - fizzle_empty_result (bool): fizzle if nothing found, default: True
        - manifest_file (str): default: None. If specififed, then manifest of
            dataset is written in .yaml or .json format, depending on the
            file name suffix.

        Fields 'uri', 'dest_dir', 'manifest_file',
        may also be a dict of format { 'key': 'some->nested->fw_spec->key' }
        for looking up value within fw_spec instead.
    """
    _fw_name = 'ManifestDatasetTask'
    required_params = [
        *DtoolLookupTask.required_params,
        "uri"]
    optional_params = [
        *DtoolLookupTask.optional_params,
        "dest_dir",
        "fizzle_empty_result",
        "manifest_file",
    ]

    def _run_task_internal(self, fw_spec):
        import dtool_lookup_api
        dtool_lookup_api.core.config.Config.interactive = False

        logger = logging.getLogger(__name__)

        uri = self.get("uri", None)
        uri = from_fw_spec(uri, fw_spec)

        dest_dir = self.get("dest_dir", os.path.abspath(os.path.curdir))
        dest_dir = from_fw_spec(dest_dir, fw_spec)

        fizzle_empty_result = self.get("fizzle_empty_result", True)
        fizzle_empty_result = from_fw_spec(fizzle_empty_result, fw_spec)

        manifest_file = self.get("manifest_file", None)
        manifest_file = from_fw_spec(manifest_file, fw_spec)

        manifest = dtool_lookup_api.manifest(uri)
        if isinstance(manifest, str) and fizzle_empty_result:  # server error message
            raise ValueError(manifest)
        elif isinstance(manifest, str):
            logger.warning(manifest)
            manifest = None
        else:  # success
            logger.debug("Manifest of queried dataset:")
            _log_nested_dict(logger.debug, manifest)

            if manifest_file:
                manifest_file_path = os.path.join(dest_dir, manifest_file)
                logger.debug("Writing manifest of queried dataset to '{}'.".format(manifest_file_path))
                write_serialized(manifest, manifest_file_path)

        return manifest