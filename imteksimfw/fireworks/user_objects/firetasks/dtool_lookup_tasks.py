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

            r = self._run_task_internal(fw_spec)

        output = {
            'response': r
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


class QueryDtoolTask(DtoolLookupTask):
    """
    A Firetask to query datasets from via dtool lookup server and write the
    them to specified directory (current working directory if not specified).

    Required params:
        - query (dict): mongo db query identifying files to fetch.
          Same as within fireworks.utils.dict_mods, use '->' in dict keys
          for querying nested documents, instead of MongoDB '.' (dot) seperator.
          Do use '.' and NOT '->' within the 'sort_key' field.

    Optional params:
        - sort_key (str): sort key, sort by 'frozen_at' per default
        - sort_direction (int): sort direction, default 'pymongo.DESCENDING'
        - limit (int): maximum number of files to write, default: no limit
        - fizzle_empty_result (bool): fizzle if no file found, default: True
        - fizzle_degenerate_dataset_name (bool): fizzle if more than one of the
          resulting datasets are named equivalently, default: False
        - meta_file (str): default: None. If specififed, then metadata of
          queried files is written in .yaml or .json format, depending on the
          file name suffix.

    Fields 'sort_key', 'sort_direction', 'limit', 'fizzle_empty_result',
    'fizzle_degenerate_dataset_name', 'meta_file' may also be a dict of format
    { 'key': 'some->nested->fw_spec->key' } for looking up value within
    fw_spec instead.
    """

    _fw_name = 'QueryDatasetTask'
    required_params = [
        *DtoolLookupTask.required_params,
        "query"]
    optional_params = [
        *DtoolLookupTask.optional_params,
        "fizzle_degenerate_dataset_name", "fizzle_empty_result",
        "limit", "meta_file", "meta_file_suffix",
        "sort_direction", "sort_key"]

    def _run_task_internal(self, fw_spec):
        import dtool_lookup_api
        dtool_lookup_api.core.config.Config.interactive = False

        logger = logging.getLogger(__name__)

        query = self.get("query", {})
        query = from_fw_spec(query, fw_spec)

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

        # if isinstance(query, dict):
        #     query = arrow_to_dot(query)
        #     # convert to query string
        #     query_str = json.dumps(query)
        # elif isinstance(query, str):
        #     query_str = query
        # else:
        #     raise ValueError(
        #         "query type('{}') is {}, but must be either dict or str".format(
        #             query, type(query)
        #         ))

        # logger.info("Query string: '{}'".format(query_str))

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

        if meta_file:
            logger.debug("Write response to file '{}'.".format(meta_file))
            write_serialized(r, meta_file)

        return r
