# coding: utf-8
#
# multiprocessing.py
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
"""Extensions to fireworks dataflow tasks."""

import io
import logging

from typing import Dict, List

from abc import abstractmethod
from contextlib import ExitStack

from fireworks.core.firework import FiretaskBase, FWAction
from fireworks.fw_config import FW_LOGGING_FORMAT
from fireworks.utilities.fw_serializers import ENCODING_PARAMS
from fireworks.utilities.dict_mods import get_nested_dict_value

from imteksimfw.utils.environ import TemporaryOSEnviron
from imteksimfw.utils.dict import compare
from imteksimfw.utils.logging import LoggingContext, _log_nested_dict
from imteksimfw.utils.multiprocessing import RunAsChildProcessTask

__author__ = 'Johannes Laurin Hoermann'
__copyright__ = 'Copyright 2020, IMTEK Simulation, University of Freiburg'
__email__ = 'johannes.hoermann@imtek.uni-freiburg.de, johannes.laurin@gmail.com'
__date__ = 'Nov 28, 2020'

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


# TODO: This follows almost the same pattern as DtoolTask, further abstraction possible
class DataflowTask(RunAsChildProcessTask):
    """
    A dtool lookup task ABC.

    Required params:
        None
    Optional params:
        - output_key (str): spec key that will be used to pass a task's output
            to child fireworks. Default: None
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
        output_key = self.get('output_key', None)
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


class SearchDictTask(DataflowTask):
    """
    Search for 'search' within 'input' and return list of matching keys (or indices in case of list) as 'output'.

    Required params:
        None
    Optional params:
        - input (dict or list): dict or list to search
        = input_key (str): if specified, then supersedes 'input' by entry in fw_spec this key points to.
            One of 'input' and 'input_key' must be specified.
        - search (obj): object to search for in 'input'.
        - search_key (str): if specified, then supersedes 'search' by entry in fw_spec this key points to.
            One of 'search' and 'search_key' must be specified.
        - marker (dict or list):  if specified, must mimic structure of entries in 'input' and mark fields for.
            comparison with boolean values. If None (default), then compare everything.
        - marker_key (str):  if specified, then supersedes 'marker' by entry in fw_spec this key points to.
            One of 'marker' and 'marker_key' must be specified.
        - output_key (str): spec key that will be used to pass the matching key or index or
            a list of matching keys or indices to child fireworks. Default: None
        - limit (int): limit the number of results. If None, then no limit (default).
        - expand (bool): will replace list result with single value result if list result only contains one entry and
            with None if list result is empty. Default: False.

    Fields 'limit', 'expand_one' may also be a dict of format { 'key': 'some->nested->fw_spec->key' }
    for looking up value within fw_spec instead.
    """
    _fw_name = 'SearchDiskTask'
    required_params = [*DataflowTask.required_params]
    optional_params = [
        *DataflowTask.optional_params,
        "input",  # dict or list to search
        "input_key",
        "search",  # entry to search for
        "search_key",
        "marker",  # marker must mimic structure of entries in input and mark fields for comparison with boolean values.
        "marker_key"]

    def _run_task_internal(self, fw_spec):
        logger = logging.getLogger(__name__)

        input = self.get('input', None)
        input_key = self.get('input_key', None)

        search = self.get('search', None)
        search_key = self.get('search_key', None)

        marker = self.get('marker', None)
        marker_key = self.get('marker_key', None)

        limit = self.get('limit', None)
        limit = from_fw_spec(limit, fw_spec)

        expand = self.get('expand', None)
        expand = from_fw_spec(expand, fw_spec)

        if input_key:
            logger.debug("input from fw_spec at '%s'." % input_key)
            input = get_nested_dict_value(fw_spec, input_key)
        elif input:
            pass
        else:
            raise ValueError("Neither 'input' nor 'input_key' specified.")

        if search_key:
            logger.debug("search from fw_spec at '%s'." % search_key)
            search = get_nested_dict_value(fw_spec, search_key)
        elif input:
            pass
        else:
            raise ValueError("Neither 'search' nor 'search_key' specified.")

        if marker_key:
            logger.debug("marker from fw_spec at '%s'." % marker_key)
            marker = get_nested_dict_value(fw_spec, marker_key)
        elif input:
            pass
        else:
            logger.warning("Neither 'marker' nor 'marker_key' specified, compare everything.")

        logger.debug("input:")
        _log_nested_dict(logger.debug, input)

        logger.debug("search:")
        _log_nested_dict(logger.debug, search)

        logger.debug("marker:")
        _log_nested_dict(logger.debug, marker)

        matches = []
        def find_match(key, entry):
            if compare(entry, search, marker):
                logger.info("Found match at %s: %s" % (key, entry))
                matches.append(key)

        if isinstance(input, dict):
            for key, entry in input.items():
                find_match(key, entry)
        elif isinstance(input, list):
            for key, entry in enumerate(input):
                find_match(key, entry)
        else:
            ValueError("type of 'input' is '%s', but must be 'dict' or 'list'." % type(input))

        logger.info("Found matches at '%s'" % matches)

        if isinstance(limit, int) and limit >=0 :
            matches = matches[:limit]
            logger.debug("Limit matches to first %d: '%s'" % (limit, matches))

        if expand and len(matches) == 1:
            matches = matches[0]
            logger.debug("Expand single-entry result'%s'." % matches)
        elif expand and len(matches) == 0:
            matches = None
            logger.debug("Expand empty result as None.")

        logger.info("Return '%s'" % matches)
        return matches
