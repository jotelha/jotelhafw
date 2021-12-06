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

import glob
import io
import logging
import os

from typing import Dict, List

from abc import abstractmethod
from contextlib import ExitStack

from fireworks.core.firework import Firework, FireTaskBase, FWAction, Workflow
from fireworks.fw_config import FW_LOGGING_FORMAT
from fireworks.utilities.fw_serializers import load_object, ENCODING_PARAMS
from fireworks.utilities.dict_mods import get_nested_dict_value, set_nested_dict_value

from imteksimfw.utils.dict import compare, dict_merge, apply_mod_spec
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
    A dataflow task ABC.

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
    _fw_name = 'DataflowTask'
    required_params = [*RunAsChildProcessTask.required_params]
    optional_params = [
        *RunAsChildProcessTask.optional_params,
        "stored_data",
        "output_key",
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

            output = self._run_task_internal(fw_spec)

        if isinstance(output, FWAction):
            fw_action = output
        else:
            fw_action = FWAction()

            if stored_data:
                fw_action.stored_data = {}
                fw_action.stored_data['output'] = output

            if output_key:  # inject into fw_spec
                fw_action.mod_spec = [{dict_mod: {output_key: output}}]

        # ATTENTION: might override stored_data, stdlog set in task
        if store_stdlog and fw_action.stored_data is None:
            fw_action.stored_data = {}
        if store_stdlog:
            stdlog_stream.flush()
            fw_action.stored_data['stdlog'] = stdlog_stream.getvalue()

        # 'propagate' only development feature for now
        if hasattr(fw_action, 'propagate') and propagate:
            fw_action.propagate = propagate

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
        "marker_key",
        "limit",
        "expand",
    ]

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


# Overrides for default Fireworks dataflow tasks

class ExtendedForeachTask(DataflowTask):
    """
    This firetask branches the workflow creating parallel fireworks
    using FWAction: one firework for each element or each chunk from the
    *split* list. Each firework in this generated list contains the firetask
    specified in the *task* dictionary. If the number of chunks is specified
    the *split* list will be divided into this number of chunks and each
    chunk will be processed by one of the generated child fireworks.

    Required params:
        - task (dict): a dictionary version of the firetask
        - split (str or [str]): label  an input list or a list of such;
          they must be available both in
          the *inputs* list of the specified task and in the spec.

    Optional params:
        - number of chunks (int): if provided the *split* input list will be
          divided into this number of sublists and each will be processed by
          a separate child firework
    """
    _fw_name = 'ExtendedForeachTask'
    required_params = [
        *DataflowTask.required_params,
        "task",
        "split"]
    optional_params = [
        *DataflowTask.optional_params,
        "number of chunks"
    ]

    def _run_task_internal(self, fw_spec):
        assert isinstance(self['split'], (str,list)), self['split']
        split_list = self['split']
        if isinstance( split_list, str): split_list = [split_list]

        reflen = 0
        for split in split_list:
            assert isinstance(fw_spec[split], list)
            #if isinstance(self['task']['inputs'], list):
            #    assert split in self['task']['inputs']
            #else: # only one inputs entry , str
            #    assert split == self['task']['inputs']

            split_field = fw_spec[split]
            lensplit = len(split_field)

            # update reflen on first iteration
            if reflen == 0:
                assert lensplit != 0, ('input to split is empty:', split)
                reflen = lensplit
                nchunks = self.get('number of chunks')
                if not nchunks:
                    nchunks = lensplit
                chunklen = lensplit // nchunks
                if lensplit % nchunks > 0:
                    chunklen = chunklen + 1

                chunks = [ { split: split_field[i:i+chunklen] } for i in range(0, lensplit, chunklen)]
            else:
                assert lensplit == reflen, ('input lists not of equal length:', split)
                for i in range(0, lensplit, chunklen):
                    chunks[i//chunklen].update( { split: split_field[i:i+chunklen] } )

        fireworks = []
        chunk_index_spec = self.get('chunk index spec')

        # allow for multiple tasks
        task_list = self['task']
        if not isinstance( task_list, list ):
            task_list = [ task_list ]
        for index, chunk in enumerate(chunks):
            spec = fw_spec.copy()
            for split in split_list:
                spec[split] = chunk[split]

            tasks = []
            for task_entry in task_list:
                task = load_object(task_entry)
                task['chunk_number'] = index
                tasks.append(task)

            if chunk_index_spec and isinstance(chunk_index_spec, str):
                spec[chunk_index_spec] = index
            name = self._fw_name + ' ' + str(index)
            fireworks.append(Firework(tasks, spec=spec, name=name))
        return FWAction(detours=fireworks)


class BranchWorkflowTask(DataflowTask):
    """
    Similarly to the ForeachTask, this task branches the workflow creating
    parallel fireworks using FWAction: instead of one firework for each element
    or each chunk from the *split* list, however, arbitrary subworkflows of
    multiple fireworks may be appended as additions or detours for each chunk.
    If the number of chunks is specified
    the *split* list will be divided into this number of chunks and each
    chunk will be processed by one of the generated child fireworks.

    Required parameters:
        - split (str or [str]): label  an input list or a list of such;
          they must be available both in
          the *inputs* list of the specified task and in the spec.

    Optional parameters:
        - number of chunks (int): if provided the *split* input list will be
          divided into this number of sublists and each will be processed by
          a separate child firework

        - addition_wf (dict): Workflow or single FireWork to always append as
            an addition. Default: None.
        - detour_wf (dict): Workflow or single FireWork to always append as
            a detour. Default: None

        NOTE: at least one of addition_wf and detour_wf must be specified.

        - detour_fws_root ([int]): fw_ids (referring to fws in detour_wf)
            to identify "roots" to be connected to this fw. If not
            specified, all dangling roots are connected.
        - detour_fws_leaf ([int]): fw_ids (referring to fws in detour_wf)
            to identify "leaves" to be connected to this fw's direct children.
            If not specified, all dangling leaves are connected.
        - addition_fws_root ([int]): fw_ids (referring to fws in addition_wf)
            to identify "roots" to be connected to this fw. If not
            specified, all dangling roots are connected.

        - apply_mod_spec_to_addition_wf (bool): Apply FWAction's update_spec and
            mod_spec to 'addition_wf', same as for all other regular childern
            of this task's FireWork. Default: True.
        - apply_mod_spec_to_detour_wf (bool): Apply FWAction's update_spec and
            mod_spec to 'detour_wf', , same as for all other regular children
            of this task's FireWork. Default: True.
        - superpose_addition_on_my_fw_spec (bool):
            merge own fw_spec with fw_spec of all FireWorks within addition_wf,
            with latter enjoying precedence. Default: False.
        - superpose_detour_on_my_fw_spec (bool):
            Merge own fw_spec with fw_spec of all FireWorks within detour_wf,
            with latter enjoying precedence. Default: False.
        - addition_fw_spec_to_exclude ([str]):
        - detour_fw_spec_to_exclude ([str]):
            When any of the above superpose flags is set, the top-level fw_spec
            fields specified here won't enter the created additions.
            Default for are all reserved fields, i.e. [
                '_add_fworker',
                '_add_launchpad_and_fw_id',
                '_allow_fizzled_parents',
                '_background_tasks',
                '_category',
                '_dupefinder',
                '_files_in',
                '_files_out',
                '_files_prev',
                '_fizzled_parents',
                '_fw_env',
                '_fworker',
                '_job_info',
                '_launch_dir',
                '_pass_job_info',
                '_preserve_fworker',
                '_priority',
                '_queueadapter',
                '_tasks',
                '_trackers',
            ]

        - output (str): spec key that will be used to pass output to child
            fireworks. Default: None
        - dict_mod (str, default: '_set'): how to insert output into output
            key, see fireworks.utils.dict_mods
        - propagate (bool, default: None): if True, then set the
            FWAction 'propagate' flag and propagate updated fw_spec not only to
            direct children, but to all descendants down to wokflow's leaves.
        - stored_data (bool, default: False): put outputs into database via
            FWAction.stored_data
        - store_stdlog (bool, default: False): insert log output into database
            (only if 'stored_data' or 'output' is spcified)
        - stdlog_file (str, Default: NameOfTaskClass.log): print log to file
        - loglevel (str, Default: logging.INFO): loglevel for this task

    Fields  'ignore_errors'
    may also be a dict of format { 'key': 'some->nested->fw_spec->key' } for
    looking up value within 'fw_spec' instead.

    NOTE: reserved fw_spec keywords are (alphabetically)
        - _add_fworker
        - _add_launchpad_and_fw_id
        - _allow_fizzled_parents
        - _background_tasks
        - _category
        - _dupefinder
        - _files_in
        - _files_out
        - _files_prev
        - _fizzled_parents
        - _fw_env
        - _fworker
        - _job_info
        - _launch_dir
        - _pass_job_info
        - _preserve_fworker
        - _priority
        - _queueadapter
        - _tasks
        - _trackers
    """
    _fw_name = 'BranchWorkflowTask'
    required_params = [
        *DataflowTask.required_params,
        "split"]
    optional_params = [
        *DataflowTask.optional_params,
        "number of chunks",

        "detour_wf",
        "addition_wf",
        "detour_fws_root",
        "detour_fws_leaf",
        "addition_fws_root",
        "apply_mod_spec_to_addition_wf",
        "apply_mod_spec_to_detour_wf",

        "superpose_addition_on_my_fw_spec",
        "superpose_detour_on_my_fw_spec",
        "addition_fw_spec_to_exclude",
        "detour_fw_spec_to_exclude",
    ]

    def appendable_wf_from_dict(self, obj_dict, base_spec=None, exclusions={}):
        """Creates Workflow from a Workflow or single FireWork dict description.

        If specified, use base_spec for all fw_spec and superpose individual
        specs on top.

        Args:
            - obj_dict (dict): describes either single FW or whole Workflow
            - base_spec (dict): use those specs for all FWs within workflow.
                Specific specs already set within obj_dict take precedence.
            - exclusions (dict): nested dict with keys marked for exclusion
                by True boolean value. Excluded keys are stripped off base_spec.

        Returns:
            (Workflow, dict) tuple
            - Worfklow: created workflow
            - {int: int}: old to new fw_ids mapping
        """
        logger = logging.getLogger(__name__)

        logger.debug("Initial obj_dict:")
        _log_nested_dict(logger.debug, obj_dict)

        if base_spec:
            logger.debug("base_spec:")
            _log_nested_dict(logger.debug, base_spec)

        if exclusions:
            logger.debug("exclusions:")
            _log_nested_dict(logger.debug, exclusions)

        remapped_fw_ids = {}
        if isinstance(obj_dict, dict):
            # in case of single Fireworks:
            if "spec" in obj_dict:
                # append firework (defined as dict):
                if base_spec:
                    obj_dict["spec"] = dict_merge(base_spec, obj_dict["spec"],
                                                  exclusions=exclusions)
                fw = Firework.from_dict(obj_dict)
                remapped_fw_ids[fw.fw_id] = self.consecutive_fw_id
                fw.fw_id = self.consecutive_fw_id
                self.consecutive_fw_id -= 1
                wf = Workflow([fw])
            else:   # if no single fw, then wf
                if base_spec:
                    for fw_dict in obj_dict["fws"]:
                        fw_dict["spec"] = dict_merge(base_spec, fw_dict["spec"],
                                                     exclusions=exclusions)
                wf = Workflow.from_dict(obj_dict)
                # do we have to reassign fw_ids? yes
                for fw in wf.fws:
                    remapped_fw_ids[fw.fw_id] = self.consecutive_fw_id
                    fw.fw_id = self.consecutive_fw_id
                    self.consecutive_fw_id -= 1
                wf._reassign_ids(remapped_fw_ids)
        else:
            raise ValueError("type({}) is '{}', but 'dict' expected.".format(
                             obj_dict, type(obj_dict)))
        logger.debug("Built object:")
        _log_nested_dict(logger.debug, wf.as_dict())

        return wf, remapped_fw_ids

    # if the curret fw yields outfiles, then check whether according
    # '_files_prev' must be written for newly created insertions
    def write_files_prev(self, wf, fw_spec, root_fw_ids=None):
        "Sets _files_prev in roots of new workflow according to _files_out in fw_spec."
        logger = logging.getLogger(__name__)

        if fw_spec.get("_files_out"):
            logger.info("Current FireWork's '_files_out': {}".format(
                        fw_spec.get("_files_out")))

            files_prev = {}

            for k, v in fw_spec.get("_files_out").items():
                files = glob.glob(os.path.join(os.curdir, v))
                if files:
                    logger.info("This Firework provides {}: {}".format(
                                k, files), " within _files_out.")
                    filepath = os.path.abspath(sorted(files)[-1])
                    logger.info("{}: '{}' provided as '_files_prev'".format(
                                k, filepath), " to subsequent FireWorks.")
                    files_prev[k] = filepath

            # get roots of insertion wf and assign _files_prev to them
            if root_fw_ids is None:
                root_fw_ids = wf.root_fw_ids
            root_fws = [fw for fw in wf.fws if fw.fw_id in root_fw_ids]

            for root_fw in root_fws:
                root_fw.spec["_files_prev"] = files_prev

        return wf

    def _run_task_internal(self, fw_spec):
        logger = logging.getLogger(__name__)

        # from original ForeachTask
        assert isinstance(self['split'], (str, list)), self['split']
        split_list = self['split']
        if isinstance(split_list, str): split_list = [split_list]
        logger.debug("Iteratring through '{}'.", split_list)
        reflen = 0
        for split in split_list:
            logger.debug("Splitting field '{}'.", split)
            assert isinstance(fw_spec[split], list)
            # if isinstance(self['task']['inputs'], list):
            #    assert split in self['task']['inputs']
            # else: # only one inputs entry , str
            #    assert split == self['task']['inputs']

            split_field = fw_spec[split]
            logger.debug("Field content '{}'.".format(split_field))
            lensplit = len(split_field)
            logger.debug("Length {}.".format(lensplit))

            # update reflen on first iteration
            if reflen == 0:
                assert lensplit != 0, ('input to split is empty:', split)
                reflen = lensplit
                nchunks = self.get('number of chunks')
                if not nchunks:
                    nchunks = lensplit
                chunklen = lensplit // nchunks
                if lensplit % nchunks > 0:
                    chunklen = chunklen + 1

                chunks = [{split: split_field[i:i + chunklen]} for i in range(0, lensplit, chunklen)]
            else:
                assert lensplit == reflen, ('input lists not of equal length:', split)
                for i in range(0, lensplit, chunklen):
                    chunks[i // chunklen].update({split: split_field[i:i + chunklen]})

            logger.debug("Split into {} chunks of length {}: {}.".format(nchunks, chunklen, chunk))

        chunk_index_spec = self.get('chunk index spec')

        # following adaptedfrom RecoverTask

        # NOTE: be careful to distinguish between what is referred to as
        # detour_wf in the task's docstring, stored within detour_wf_dict below
        # and the final detour_wf constructed, possibly comprising the partial
        # restart and detour workflows specified via task parameters as well as
        # another copy of this recover_fw.

        self.consecutive_fw_id = -1  # quite an ugly necessity
        # get fw_spec entries or their default values:
        detour_wf_dict = self.get('detour_wf', None)
        addition_wf_dict = self.get('addition_wf', None)

        detour_fws_root = self.get('detour_fws_root', None)
        detour_fws_leaf = self.get('detour_fws_leaf', None)
        addition_fws_root = self.get('addition_fws_root', None)

        apply_mod_spec_to_addition_wf = self.get('apply_mod_spec_to_addition_wf', True)
        apply_mod_spec_to_addition_wf = from_fw_spec(apply_mod_spec_to_addition_wf,
                                                     fw_spec)

        apply_mod_spec_to_detour_wf = self.get('apply_mod_spec_to_detour_wf', True)
        apply_mod_spec_to_detour_wf = from_fw_spec(apply_mod_spec_to_detour_wf,
                                                   fw_spec)

        superpose_addition_on_my_fw_spec = self.get(
            'superpose_addition_on_my_fw_spec', False)
        superpose_addition_on_my_fw_spec = from_fw_spec(
            superpose_addition_on_my_fw_spec, fw_spec)

        superpose_detour_on_my_fw_spec = self.get(
            'superpose_detour_on_my_fw_spec', False)
        superpose_detour_on_my_fw_spec = from_fw_spec(
            superpose_detour_on_my_fw_spec, fw_spec)

        default_fw_spec_to_exclude = [
            '_add_fworker',
            '_add_launchpad_and_fw_id',
            '_allow_fizzled_parents',
            '_background_tasks',
            '_category',
            '_dupefinder',
            '_files_in',
            '_files_out',
            '_files_prev',
            '_fizzled_parents',
            '_fw_env',
            '_fworker',
            '_job_info',
            '_launch_dir',
            '_pass_job_info',
            '_preserve_fworker',
            '_priority',
            '_queueadapter',
            '_tasks',
            '_trackers',
        ]

        addition_fw_spec_to_exclude = self.get('addition_fw_spec_to_exclude', default_fw_spec_to_exclude)
        if isinstance(addition_fw_spec_to_exclude, list):
            addition_fw_spec_to_exclude_dict = {k: True for k in addition_fw_spec_to_exclude}
        else:  # supposed to be dict then
            addition_fw_spec_to_exclude_dict = addition_fw_spec_to_exclude

        detour_fw_spec_to_exclude = self.get('detour_fw_spec_to_exclude', default_fw_spec_to_exclude)
        if isinstance(detour_fw_spec_to_exclude, list):
            detour_fw_spec_to_exclude_dict = {k: True for k in detour_fw_spec_to_exclude}
        else:  # supposed to be dict then
            detour_fw_spec_to_exclude_dict = detour_fw_spec_to_exclude

        # input assertions, ATTENTION: order matters

        # find other files to forward:
        file_list = []

        # distinguish between FireWorks and Workflows by top-level keys
        # fw: ['spec', 'fw_id', 'created_on', 'updated_on', 'name']
        # wf: ['fws', 'links', 'name', 'metadata', 'updated_on', 'created_on']
        detour_wf_list = []
        addition_wf_list = []

        all_mapped_detour_fws_root = []
        all_mapped_detour_fws_leaf = []
        all_mapped_addition_fws_root = []

        fw_action = FWAction()

        for index, chunk in enumerate(chunks):
            minimal_base_spec = {}
            full_base_spec = fw_spec.copy()
            for split in split_list:
                minimal_base_spec[split] = chunk[split]
                full_base_spec[split] = chunk[split]

            minimal_base_spec['chunk_number'] = index
            full_base_spec['chunk_number'] = index

            if chunk_index_spec and isinstance(chunk_index_spec, str):
                minimal_base_spec[chunk_index_spec] = index
                full_base_spec[chunk_index_spec] = index

            detour_wf = None
            addition_wf = None
            mapped_detour_fws_root = []
            mapped_detour_fws_leaf = []
            mapped_addition_fws_root = []

            # build detour wf
            if isinstance(detour_wf_dict, dict):
                if superpose_detour_on_my_fw_spec:
                    detour_wf_base_spec = full_base_spec.copy()
                else:
                    detour_wf_base_spec = minimal_base_spec.copy()

                logger.debug("Base spec for detour {}:".format(index))
                _log_nested_dict(detour_wf_base_spec)

                detour_wf, detour_wf_fw_id_mapping = self.appendable_wf_from_dict(
                    detour_wf_dict, base_spec=detour_wf_base_spec,
                    exclusions=detour_fw_spec_to_exclude_dict)

                if detour_fws_root is None:  # default, as in core fireworks
                    mapped_detour_fws_root.extend(detour_wf.root_fw_ids)
                elif isinstance(detour_fws_root, (list, tuple)):
                    mapped_detour_fws_root.extend(
                        [detour_wf_fw_id_mapping[fw_id] for fw_id in detour_fws_root])
                else:  # isinstance(detour_fws_root, int)
                    mapped_detour_fws_root.append(detour_wf_fw_id_mapping[detour_fws_root])

                if detour_fws_leaf is None:  # default, as in core fireworks
                    mapped_detour_fws_leaf.extend(detour_wf.leaf_fw_ids)
                elif isinstance(detour_fws_leaf, (list, tuple)):
                    mapped_detour_fws_leaf.extend(
                        [detour_wf_fw_id_mapping[fw_id] for fw_id in detour_fws_leaf])
                else:  # isinstance(detour_fws_leaf, int)
                    mapped_detour_fws_leaf.append(detour_wf_fw_id_mapping[detour_fws_leaf])

                # only log if sepcific roots or leaves had been specified
                if detour_fws_root:
                    logger.debug("Mapped detour_wf root fw_ids {} onto newly created fw_ids {}".format(
                        detour_fws_root, mapped_detour_fws_root[-len(detour_fws_root):]))
                if detour_fws_leaf:
                    logger.debug("Mapped detour_wf leaf fw_ids {} onto newly created fw_ids {}".format(
                        detour_fws_leaf, mapped_detour_fws_leaf[-len(detour_fws_leaf)]))

                if apply_mod_spec_to_detour_wf:
                    apply_mod_spec(detour_wf, fw_action, fw_ids=mapped_detour_fws_root)

                self.write_files_prev(detour_wf, fw_spec, root_fw_ids=mapped_detour_fws_root)
                logger.debug("detour_wf:")
                _log_nested_dict(logger.debug, detour_wf.as_dict())

                detour_wf_list.append(detour_wf)
                all_mapped_detour_fws_root.append(mapped_detour_fws_root)
                all_mapped_detour_fws_leaf.append(mapped_detour_fws_leaf)

            # addition wf
            if isinstance(addition_wf_dict, dict):
                if superpose_addition_on_my_fw_spec:
                    addition_wf_base_spec = full_base_spec.copy()
                else:
                    addition_wf_base_spec = minimal_base_spec.copy()

                logger.debug("Base spec for addition {}:".format(index))
                _log_nested_dict(addition_wf_base_spec)

                addition_wf, addition_wf_fw_id_mapping = self.appendable_wf_from_dict(
                    addition_wf_dict, base_spec=addition_wf_base_spec,
                    exclusions=addition_fw_spec_to_exclude_dict)

                if addition_fws_root is None:
                    mapped_addition_fws_root.extend(addition_wf.root_fw_ids)
                elif isinstance(addition_fws_root, (list, tuple)):
                    mapped_addition_fws_root.extend(
                        [addition_wf_fw_id_mapping[fw_id] for fw_id in addition_fws_root])
                else:  # isinstance(addition_fws_root, int)
                    mapped_addition_fws_root.append(addition_wf_fw_id_mapping[addition_fws_root])

                if addition_fws_root:
                    logger.debug("Mapped addition_wf root fw_ids {} onto newly created fw_ids {}".format(
                        addition_fws_root, mapped_addition_fws_root[-len(addition_fws_root):]))

                self.write_files_prev(addition_wf, fw_spec, root_fw_ids=mapped_addition_fws_root)
                logger.debug("addition_wf:")
                _log_nested_dict(logger.debug, addition_wf.as_dict())

                if apply_mod_spec_to_addition_wf:
                    apply_mod_spec(addition_wf, fw_action, fw_ids=mapped_addition_fws_root)

                addition_wf_list.append(addition_wf)
                all_mapped_addition_fws_root.append(mapped_addition_fws_root)

        # if an action's detour is not None but empty list, then fireworks
        # throws
        #   File ".../site-packages/fireworks/core/firework.py", line 185, in __init__
        #     elif not isinstance(self.detours_root_fw_ids[0], (list, tuple)):
        #       IndexError: list index out of range"
        if len(detour_wf_list) > 0:
            for index, detour_wf in enumerate(detour_wf_list):
                logger.debug("detour_wf {}:".format(index))
            _log_nested_dict(logger.debug, detour_wf)

            fw_action.detours = detour_wf_list
            fw_action.detours_root_fw_ids = all_mapped_detour_fws_root
            fw_action.detours_leaf_fw_ids = all_mapped_detour_fws_leaf

        if len(addition_wf_list) > 0:
            for index, addition_wf in enumerate(addition_wf_list):
                logger.debug("addition_wf {}:".format(index))
            _log_nested_dict(logger.debug, addition_wf)

            fw_action.additions = addition_wf_list
            fw_action.additions_root_fw_ids = all_mapped_addition_fws_root

        return fw_action


class EnhancedJoinDictTask(FireTaskBase):
    """ combines specified spec fields into a dictionary """
    _fw_name = 'EnhancedJoinDictTask'
    required_params = ['inputs', 'output']
    optional_params = ['rename']

    def run_task(self, fw_spec):
        assert isinstance(self['output'], str)
        assert isinstance(self['inputs'], list)

        try:  # replace if / esle with try / except to find possibly nested val
            output = get_nested_dict_value(fw_spec, self['output'])
        except KeyError:
            output = {}

        assert isinstance(output, dict), "output must be dict."

        if self.get('rename'):
            assert isinstance(self.get('rename'), dict)
            rename = self.get('rename')
        else:
            rename = {}
        for item in self['inputs']:
            if item in rename:
                output = set_nested_dict_value(
                    output, self['rename'][item],
                    get_nested_dict_value(fw_spec, item))
                # replaces
                # output[self['rename'][item]] = fw_spec[item]
            else:
                output = set_nested_dict_value(
                    output, item,
                    get_nested_dict_value(fw_spec, item))
                # replaces
                # output[item] = fw_spec[item]

        return FWAction(mod_spec=[{'_set': {self['output']: output}}])
        # replaces
        # return FWAction(update_spec={self['output']: output})


class EnhancedJoinListTask(FireTaskBase):
    """ combines specified spec fields into a list. """
    _fw_name = 'EnhancedJoinListTask'
    required_params = ['inputs', 'output']

    def run_task(self, fw_spec):
        assert isinstance(self['output'], str)
        assert isinstance(self['inputs'], list)

        try:  # replace if / esle with try / except to find possibly nested val
            output = get_nested_dict_value(fw_spec, self['output'])
        except KeyError:
            output = []
        assert isinstance(output, list), "output must be list."
        # replaces
        # if self['output'] not in fw_spec:
        #    output = []
        # else:
        #    assert isinstance(fw_spec[self['output']], list)
        #    output = fw_spec[self['output']]

        for item in self['inputs']:
            output.append(get_nested_dict_value(fw_spec, item))
            # replaces
            # output.append(fw_spec[item])

        return FWAction(mod_spec=[{'_set': {self['output']: output}}])
        # replaces
        # return FWAction(update_spec={self['output']: output})