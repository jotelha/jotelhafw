# coding: utf-8
#
# recover_tasks.py
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
"""Tasks that recover and restart failed computations from checkpoints."""

import glob
import io
import logging
import os
import shutil

from collections.abc import Iterable
from contextlib import ExitStack

from fireworks.fw_config import FW_LOGGING_FORMAT
from fireworks.utilities.fw_serializers import ENCODING_PARAMS
from fireworks.utilities.dict_mods import get_nested_dict_value, set_nested_dict_value
from fireworks.core.firework import FWAction, FiretaskBase, Firework, Workflow

from imteksimfw.utils.logging import LoggingContext, _log_nested_dict
from imteksimfw.utils.dict import dict_merge, from_fw_spec, apply_mod_spec


__author__ = 'Johannes Laurin Hoermann'
__copyright__ = 'Copyright 2020, IMTEK Simulation, University of Freiburg'
__email__ = 'johannes.hoermann@imtek.uni-freiburg.de, johannes.laurin@gmail.com'
__date__ = 'August 10, 2020'

DEFAULT_FORMATTER = logging.Formatter(FW_LOGGING_FORMAT)


class RecoverTask(FiretaskBase):
    """
    Generic base class for recovering and restarting some computation.

    The typical use case is some FIZZLED run, i.e. due to exceeded walltime.
    Activate _allow_fizzled_parents and append to initial run Firework.

    If inserted by means of some 'recovery fw' between a parent and its children

        +--------+     +------------+
        | parent | --> | child(ren) |
        +--------+     +------------+

    as shown

        +--------+     +-------------+     +------------+
        | parent | --> | recovery fw | --> | child(ren) |
        +--------+     +-------------+     +------------+


    then this task generates the following insertion in case of the parent's
    failure

                                                       +- - - - - - - - - - - - - - - -+
                                                       ' detour_wf                     '
                                                       '                               '
                                                       ' +-----------+     +---------+ '
                                 +-------------------> ' |  root(s)  | --> | leaf(s) | ' ------+
                                 |                     ' +-----------+     +---------+ '       |
                                 |                     '                               '       |
                                 |                     +- - - - - - - - - - - - - - - -+       |
                                 |                                                             |
                                 |                                                             |
                                 |                                                             |
                                                       +- - - - - - - - - - - - - - - -+       |
                                                       ' restart_wf                    '       |
                                                       '                               '       v
        +----------------+     +-----------------+     ' +-----------+     +---------+ '     +-----------------+     +------------+
        | fizzled parent | --> | 1st recovery fw | --> ' |  root(s)  | --> | leaf(s) | ' --> | 2nd recovery fw | --> | child(ren) |
        +----------------+     +-----------------+     ' +-----------+     +---------+ '     +-----------------+     +------------+
                                                       '                               '
                                                       +- - - - - - - - - - - - - - - -+
                                 |
                                 |
                                 |
                                 |                     +- - - - - - - - - - - - - - - -+
                                 |                     ' addition_wf                   '
                                 |                     '                               '
                                 |                     ' +-----------+     +---------+ '
                                 +-------------------> ' |  root(s)  | --> | leaf(s) | '
                                                       ' +-----------+     +---------+ '
                                                       '                               '
                                                       +- - - - - - - - - - - - - - - -+

    This dynamoc insertion repeats until 'restart_wf' completes successfully
    or the number of repetitions reaches 'max_restarts'. While 'restart_wf'
    is only appended in the case of a parent's failure, 'detour_wf' and
    'addition_wf' are always inserted:

        + - - - - - - - - - - - -+                              + - - - - - - - - - - - - - - - +
        ' successfull restart_wf '                              ' detour_wf                     '
        '                        '                              '                               '
        '            +---------+ '     +------------------+     ' +-----------+     +---------+ '     +------------+
        '   ...  --> | leaf(s) | ' --> | last recovery fw | --> ' |  root(s)  | --> | leaf(s) | ' --> | child(ren) |
        '            +---------+ '     +------------------+     ' +-----------+     +---------+ '     +------------+
        '                        '                              '                               '
        + - - - - - - - - - - - -+                              + - - - - - - - - - - - - - - - +
                                                           |
                                                           |
                                                           |
                                                           |                      + - - - - - - - - - - - - - - - +
                                                           |                      ' addition_wf                   '
                                                           |                      '                               '
                                                           |                      ' +-----------+     +---------+ '
                                                           +--------------------> ' |  root(s)  | --> | leaf(s) | '
                                                                                  ' +-----------+     +---------+ '
                                                                                  '                               '
                                                                                  + - - - - - - - - - - - - - - - +

    NOTE: make sure that the used 'recovery fw' forwards all outputs
    transparently in case of parent's success.

    NOTE: while the dynamic extensions 'detour_wf' and 'addition_wf' can
    actually be a whole Workflow as well as a single FireWork, 'recover_fw'
    must be single FireWorks. If more complex constructs are necessary,
    consider generating such within those FireWorks.

    NOTE: fails for several parents if the "wrong" parent fizzles. Use only
    in unambiguous situations.

    Required parameters:
        - restart_wf (dict): Workflow or single FireWork to append only if
            restart file present (parent failed). Task will not append anything
            if None. Default: None

    Optional parameters:
        - addition_wf (dict): Workflow or single FireWork to always append as
            an addition, independent on parent's success (i.e. storage).
            Default: None.
        - detour_wf (dict): Workflow or single FireWork to always append as
            a detour, independent on parent's success (i.e. post-processing).
            Task will not append anything if None. Default: None

        - restart_fws_root ([int]): fw_ids (referring to fws in restart_wf)
            to identify "roots" to be connected to this recover_fw. If not
            specified, all dangling roots are connected.
        - restart_fws_leaf ([int]): fw_ids (referring to fws in restart_wf)
            to identify "leaves" to be connected to next recover_fw. If not
            specified, all dangling leaves are connected.
        - detour_fws_root ([int]): fw_ids (referring to fws in detour_wf)
            to identify "roots" to be connected to this recover_fw. If not
            specified, all dangling roots are connected.
        - detour_fws_leaf ([int]): fw_ids (referring to fws in detour_wf)
            to identify "leaves" to be connected to next recover_fw. If not
            specified, all dangling leaves are connected.
        - addition_fws_root ([int]): fw_ids (referring to fws in addition_wf)
            to identify "roots" to be connected to this recover_fw. If not
            specified, all dangling roots are connected.

        - apply_mod_spec_to_addition_wf (bool): Apply FWAction's update_spec and
            mod_spec to 'addition_wf', same as for all other regular childern
            of this task's FireWork. Default: True.
        - apply_mod_spec_to_detour_wf (bool): Apply FWAction's update_spec and
            mod_spec to 'detour_wf', 'restart_wf', and the next instance of
            'recover_fw', same as for all other regular childern of this task's
            FireWork. Default: True.
        - fizzle_on_no_restart_file (bool): Default: True
        - fw_spec_to_exclude ([str]): recover FireWork will inherit the current
            FireWork's 'fw_spec', stripped of top-level fields specified here.
            Default: ['_job_info', '_fw_env', '_files_prev', '_fizzled_parents']
        - ignore_errors (bool): Ignore errors when copying files. Default: True
        - max_restarts (int): Maximum number of repeated restarts (in case of
            'restart' == True). Default: 5
        - other_glob_patterns (str or [str]): Patterns for glob.glob to identify
            other files to be forwarded. All files matching this glob pattern
            are recovered. Default: None
        - repeated_recover_fw_name (str): Name for repeated recovery fireworks.
            If None, the name of this FireWorksis used. Default: None.
        - restart_counter (str): fw_spec path for restart counting.
            Default: 'restart_count'.
        - restart_file_glob_patterns (str or [str]): Patterns for glob.glob to
            identify restart files. Attention: Be careful not to match any
            restart file that has been used as an input initially.
            If more than one file matches a glob pattern in the list, only the
            most recent macth per list entry is recovered.
            Default: ['*.restart[0-9]']
        - restart_file_dests (str or [str]): Destinations of copied restart
            files. Most recent restart file found for each entry in the
            restart_file_glob_patterns list will be copied to new launchdir
            named after the according entry within this list. If None, then
            files keep their original names. Default: None
        - superpose_restart_on_parent_fw_spec (bool):
            Try to pull (fizzled) parent's fw_spec and merge with fw_spec of
            all FireWorks within restart_wf, with latter enjoying precedence.
            Default: False.
        - superpose_addition_on_parent_fw_spec (bool):
            Try to pull (fizzled) parent's fw_spec and merge with fw_spec of
            all FireWorks within addition_wf, with latter enjoying precedence.
            Default: False.
        - superpose_detour_on_parent_fw_spec (bool):
            Try to pull (fizzled) parent's fw_spec and merge with fw_spec of
            all FireWorks within detour_wf, with latter enjoying precedence.
            Default: False.
            For above's superpose oprions, fw_spec_to_exclude applies as well.

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

    Fields 'max_restarts', 'restart_file_glob_patterns', 'other_glob_patterns',
    'default_restart_file', 'fizzle_on_no_restart_file',
    'repeated_recover_fw_name', and 'ignore_errors'
    may also be a dict of format { 'key': 'some->nested->fw_spec->key' } for
    looking up value within 'fw_spec' instead.

    NOTE: reserved fw_spec keywords are
        - all reserved keywords:
        - _tasks
        - _priority
        - _pass_job_info
        - _launch_dir
        - _fworker
        - _category
        - _queueadapter
        - _add_fworker
        - _add_launchpad_and_fw_id
        - _dupefinder
        - _allow_fizzled_parents
        - _preserve_fworker
        - _job_info
        - _fizzled_parents
        - _trackers
        - _background_tasks
        - _fw_env
        - _files_in
        - _files_out
        - _files_prev
    """
    _fw_name = "RecoverTask"
    required_params = [
        "restart_wf",
    ]
    optional_params = [
        "detour_wf",
        "addition_wf",

        "restart_fws_root",
        "restart_fws_leaf",
        "detour_fws_root",
        "detour_fws_leaf",
        "addition_fws_root",

        "apply_mod_spec_to_addition_wf",
        "apply_mod_spec_to_detour_wf",
        "fizzle_on_no_restart_file",
        "fw_spec_to_exclude",
        "ignore_errors",
        "max_restarts",
        "other_glob_patterns",
        "repeated_recover_fw_name",
        "restart_counter",
        "restart_file_glob_patterns",
        "restart_file_dests",

        "superpose_restart_on_parent_fw_spec",
        "superpose_addition_on_parent_fw_spec",
        "superpose_detour_on_parent_fw_spec",

        "stored_data",
        "output",
        "dict_mod",
        "propagate",
        "stdlog_file",
        "store_stdlog",
        "loglevel"]

    def appendable_wf_from_dict(self, obj_dict, base_spec=None, exclusions={}):
        """Creates Workflow from a Workflow or single FireWork dict description.

        If specified, use base_spec for all fw_spec and superpose individual
        specs on top.

        Args:
            - obj_dict (dict): describes either single FW or whole Workflow
            - base_spec (dict): use those specs for all FWs within workflow.
                Specific specs already set within obj_dict take precedence.
            - exclusiond (dict): nested dict with keys marked for exclusion
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

    # modeled to match original snippet from fireworks.core.rocket:
    #
    # if my_spec.get("_files_out"):
    #     # One potential area of conflict is if a fw depends on two fws
    #     # and both fws generate the exact same file. That can lead to
    #     # overriding. But as far as I know, this is an illogical use
    #     # of a workflow, so I can't see it happening in normal use.
    #     for k, v in my_spec.get("_files_out").items():
    #         files = glob.glob(os.path.join(launch_dir, v))
    #         if files:
    #             filepath = sorted(files)[-1]
    #             fwaction.mod_spec.append({
    #                 "_set": {"_files_prev->{:s}".format(k): filepath}
    #             })

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

    def run_task(self, fw_spec):
        # NOTE: be careful to distinguish between what is referred to as
        # detour_wf in the task's docstring, stored within detour_wf_dict below
        # and the final detour_wf constructed, possibly comprising the partial
        # restart and detour workflows specified via task parameters as well as
        # another copy of this recover_fw.

        self.consecutive_fw_id = -1  # quite an ugly necessity
        # get fw_spec entries or their default values:
        restart_wf_dict = self.get('restart_wf', None)
        detour_wf_dict = self.get('detour_wf', None)
        addition_wf_dict = self.get('addition_wf', None)

        restart_fws_root = self.get('restart_fws_root', None)
        restart_fws_leaf = self.get('restart_fws_leaf', None)
        detour_fws_root = self.get('detour_fws_root', None)
        detour_fws_leaf = self.get('detour_fws_leaf', None)
        addition_fws_root = self.get('addition_fws_root', None)

        apply_mod_spec_to_addition_wf = self.get('apply_mod_spec_to_addition_wf', True)
        apply_mod_spec_to_addition_wf = from_fw_spec(apply_mod_spec_to_addition_wf,
                                                     fw_spec)

        apply_mod_spec_to_detour_wf = self.get('apply_mod_spec_to_detour_wf', True)
        apply_mod_spec_to_detour_wf = from_fw_spec(apply_mod_spec_to_detour_wf,
                                                   fw_spec)

        fizzle_on_no_restart_file = self.get('fizzle_on_no_restart_file', True)
        fizzle_on_no_restart_file = from_fw_spec(fizzle_on_no_restart_file,
                                                 fw_spec)

        ignore_errors = self.get('ignore_errors', True)
        ignore_errors = from_fw_spec(ignore_errors, fw_spec)

        max_restarts = self.get('max_restarts', 5)
        max_restarts = from_fw_spec(max_restarts, fw_spec)

        other_glob_patterns = self.get('other_glob_patterns', None)
        other_glob_patterns = from_fw_spec(other_glob_patterns, fw_spec)

        repeated_recover_fw_name = self.get('repeated_recover_fw_name',
                                            'Repeated LAMMPS recovery')
        repeated_recover_fw_name = from_fw_spec(repeated_recover_fw_name,
                                                fw_spec)

        restart_counter = self.get('restart_counter', 'restart_count')

        restart_file_glob_patterns = self.get('restart_file_glob_patterns',
                                              ['*.restart[0-9]'])
        restart_file_glob_patterns = from_fw_spec(restart_file_glob_patterns,
                                                  fw_spec)

        restart_file_dests = self.get('restart_file_dests', None)
        restart_file_dests = from_fw_spec(restart_file_dests, fw_spec)

        superpose_restart_on_parent_fw_spec = self.get(
            'superpose_restart_on_parent_fw_spec', False)
        superpose_restart_on_parent_fw_spec = from_fw_spec(
            superpose_restart_on_parent_fw_spec, fw_spec)

        superpose_addition_on_parent_fw_spec = self.get(
            'superpose_addition_on_parent_fw_spec', False)
        superpose_addition_on_parent_fw_spec = from_fw_spec(
            superpose_addition_on_parent_fw_spec, fw_spec)

        superpose_detour_on_parent_fw_spec = self.get(
            'superpose_detour_on_parent_fw_spec', False)
        superpose_detour_on_parent_fw_spec = from_fw_spec(
            superpose_detour_on_parent_fw_spec, fw_spec)

        fw_spec_to_exclude = self.get('fw_spec_to_exclude',
                                      [
                                        '_job_info',
                                        '_fw_env',
                                        '_files_prev',
                                        '_fizzled_parents',
                                      ])
        if isinstance(fw_spec_to_exclude, list):
            fw_spec_to_exclude_dict = {k: True for k in fw_spec_to_exclude}
        else:  # supposed to be dict then
            fw_spec_to_exclude_dict = fw_spec_to_exclude

        # generic parameters
        stored_data = self.get('stored_data', False)
        output_key = self.get('output', None)
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
                logfh = logging.FileHandler(
                    stdlog_file, mode='a', **ENCODING_PARAMS)
                logfh.setFormatter(DEFAULT_FORMATTER)
                stack.enter_context(
                    LoggingContext(handler=logfh, level=loglevel, close=True))

            logger = logging.getLogger(__name__)

            # input assertions, ATTENTION: order matters

            # avoid iterating through each character of string
            if isinstance(restart_file_glob_patterns, str):
                restart_file_glob_patterns = [restart_file_glob_patterns]

            if not restart_file_dests:
                # don't rename restart files when recovering
                restart_file_dests = os.curdir

            if isinstance(restart_file_dests, str):
                # if specified as plain string, make it an iterable list
                restart_file_dests = [restart_file_dests]

            if len(restart_file_dests) == 1:
                # if only one nenry, then all possible restart files go to that
                # destination
                restart_file_dests = restart_file_dests*len(
                    restart_file_glob_patterns)

            if len(restart_file_dests) > 1:
                # supposedly, specific destinations have been specified for
                # all possible restart files. If not:
                if len(restart_file_glob_patterns) != len(restart_file_dests):
                    logger.warning(
                        "There are {} restart_file_glob_patterns, "
                        "but {} restart_file_dests, latter ignored. "
                        "Specify none, a single or one "
                        "restart_file_dest per restart_file_glob_patterns "
                        "a Every restart file glob pattern ".format(
                            len(restart_file_glob_patterns),
                            len(restart_file_dests)))
                    # fall back to default
                    restart_file_dests = [os.curdir]*len(
                        restart_file_glob_patterns)

            # we have to decide whether the previous FireWorks failed or ended
            # successfully and then append a restart run or not

            recover = True  # per default, recover
            # check whether a previous firework handed down information
            prev_job_info = None
            path_prefix = None
            # pull from intentionally passed job info:
            if '_job_info' in fw_spec:
                job_info_array = fw_spec['_job_info']
                prev_job_info = job_info_array[-1]
                path_prefix = prev_job_info['launch_dir']
                logger.info('The name of the previous job was: {}'.format(
                    prev_job_info['name']))
                logger.info('The id of the previous job was: {}'.format(
                    prev_job_info['fw_id']))
                logger.info('The location of the previous job was: {}'.format(
                    path_prefix))
            # TODO: fails for several parents if the "wrong" parent fizzles
            # pull from fizzled previous FW:
            elif '_fizzled_parents' in fw_spec:
                fizzled_parents_array = fw_spec['_fizzled_parents']
                # pull latest (or last) fizzled parent:
                prev_job_info = fizzled_parents_array[-1]
                # pull latest launch
                path_prefix = prev_job_info['launches'][-1]['launch_dir']
                logger.info(
                    'The name of fizzled parent Firework was: {}'.format(
                        prev_job_info['name']))
                logger.info(
                    'The id of fizzled parent Firework was: {}'.format(
                        prev_job_info['fw_id']))
                logger.info(
                    'The location of fizzled parent Firework was: {}'.format(
                        path_prefix))
            else:  # no info about previous (fizzled or other) jobs
                logger.info(
                    'No information about previous (fizzled or other) jobs available.')
                recover = False  # don't recover
                # assume that parent completed successfully

            if prev_job_info and 'spec' in prev_job_info:
                logger.debug("Parent Firewor has 'spec':")
                _log_nested_dict(logger.debug, prev_job_info["spec"])
            else:
                logger.warning("Parent Firework description does not include 'spec' key.")

            # find other files to forward:
            file_list = []

            if recover:
                if not isinstance(other_glob_patterns, Iterable):
                    other_glob_patterns = [other_glob_patterns]
                for other_glob_pattern in other_glob_patterns:
                    if isinstance(other_glob_pattern, str):  # avoid non string objs
                        logger.info("Processing glob pattern {}".format(
                            other_glob_pattern))
                        file_list.extend(
                            glob.glob(
                                os.path.join(
                                    path_prefix, other_glob_pattern))
                        )

                # copy other files if necessary
                if len(file_list) > 0:
                    for f in file_list:
                        logger.info("File {} will be forwarded.".format(f))
                        try:
                            dest = os.getcwd()
                            shutil.copy(f, dest)
                        except Exception as exc:
                            if ignore_errors:
                                logger.warning("There was an error copying "
                                               "'{}' to '{}', ignored:".format(
                                                   f, dest))
                                logger.warning(exc)
                            else:
                                raise exc

                # find restart files as (src, dest) tuples:
                restart_file_list = []

                for glob_pattern, dest in zip(restart_file_glob_patterns,
                                              restart_file_dests):
                    restart_file_matches = glob.glob(os.path.join(
                        path_prefix, glob_pattern))

                    # determine most recent of restart files matches:
                    if len(restart_file_matches) > 1:
                        sorted_restart_file_matches = sorted(
                            restart_file_matches, key=os.path.getmtime)  # sort by modification time
                        logger.info("Several restart files {} (most recent last) "
                                    "for glob pattern '{}'.".format(
                                        glob_pattern,
                                        sorted_restart_file_matches))
                        logger.info("Modification times for those files: {}".format(
                            [os.path.getmtime(f) for f in sorted_restart_file_matches]))
                        logger.info("Most recent restart file '{}' will be copied "
                                    "to '{}'.".format(
                                        sorted_restart_file_matches[-1], dest))
                        restart_file_list.append(
                            (sorted_restart_file_matches[-1], dest))
                    elif len(restart_file_matches) == 1:
                        logger.info("One restart file '{}' for glob "
                                    "pattern '{}' will be copied to '{}'.".format(
                                        restart_file_matches[0],
                                        glob_pattern, dest))
                        restart_file_list.append(
                            (restart_file_matches[0], dest))
                    else:
                        logger.info("No restart file!")
                        if fizzle_on_no_restart_file:
                            raise ValueError(
                                "No restart file in {} for glob pattern {}".format(
                                    path_prefix, glob_pattern))

                # copy all identified restart files
                if len(restart_file_list) > 0:
                    for current_restart_file, dest in restart_file_list:
                        current_restart_file_basename = os.path.basename(current_restart_file)
                        logger.info("File {} will be forwarded.".format(
                            current_restart_file_basename))
                        try:
                            shutil.copy(current_restart_file, dest)
                        except Exception as exc:
                            logger.error("There was an error copying from {} "
                                         "to {}".format(
                                             current_restart_file, dest))
                            raise exc

            # distinguish between FireWorks and Workflows by top-level keys
            # fw: ['spec', 'fw_id', 'created_on', 'updated_on', 'name']
            # wf: ['fws', 'links', 'name', 'metadata', 'updated_on', 'created_on']
            detour_wf = None
            addition_wf = None
            mapped_detour_fws_root = []
            mapped_detour_fws_leaf = []
            mapped_addition_fws_root = []

            # if detour_fw given, append in any case:
            if isinstance(detour_wf_dict, dict):
                detour_wf_base_spec = None
                if superpose_detour_on_parent_fw_spec:
                    if "spec" in prev_job_info:
                        detour_wf_base_spec = prev_job_info["spec"]
                    else:
                        logger.warning("Superposition of detour_wf's "
                                       "fw_spec onto parent's "
                                       "fw_spec desired, but not parent"
                                       "fw_spec recovered.")
                detour_wf, detour_wf_fw_id_mapping = self.appendable_wf_from_dict(
                    detour_wf_dict, base_spec=detour_wf_base_spec,
                    exclusions=fw_spec_to_exclude_dict)

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

            if detour_wf is not None:
                logger.debug(
                    "detour_wf:")
                _log_nested_dict(logger.debug, detour_wf.as_dict())

            # append restart fireworks if desired
            if recover:
                # try to derive number of restart from fizzled parent
                restart_count = None
                if prev_job_info and ('spec' in prev_job_info):
                    try:
                        restart_count = get_nested_dict_value(
                            prev_job_info['spec'], restart_counter)
                    except KeyError:
                        logger.warning("Found no restart count in fw_spec of "
                                       "fizzled parent at key '{}.'".format(
                                            restart_counter))

                # if none found, look in own fw_spec
                if restart_count is None:
                    try:
                        restart_count = get_nested_dict_value(
                            prev_job_info['spec'], restart_counter)
                    except KeyError:
                        logger.warning("Found no restart count in own fw_spec "
                                       "at key '{}.'".format(restart_counter))

                # if still none found, assume it's the 1st
                if restart_count is None:
                    restart_count = 1  # original run without restart_count is "0th"
                else:  # make sure above's queried value is an integer
                    restart_count = int(restart_count) + 1  # next restart now

                if restart_count < max_restarts + 1:
                    logger.info(
                        "This is #{:d} of at most {:d} restarts.".format(
                            restart_count, max_restarts))

                    restart_wf_base_spec = None
                    if superpose_restart_on_parent_fw_spec:
                        if "spec" in prev_job_info:
                            restart_wf_base_spec = prev_job_info["spec"]
                        else:
                            logger.warning("Superposition of restart_wf's "
                                           "fw_spec onto parent's "
                                           "fw_spec desired, but not parent"
                                           "fw_spec recovered.")
                    restart_wf, restart_wf_fw_id_mapping = self.appendable_wf_from_dict(
                        restart_wf_dict, base_spec=restart_wf_base_spec,
                        exclusions=fw_spec_to_exclude_dict)

                    if restart_fws_root is None:  # default, as in core fireworks
                        mapped_detour_fws_root.extend(restart_wf.root_fw_ids)
                    elif isinstance(restart_fws_root, (list, tuple)):
                        mapped_detour_fws_root.extend(
                            [restart_wf_fw_id_mapping[fw_id] for fw_id in restart_fws_root])
                    else:  # isinstance(restart_fws_root, int)
                        mapped_detour_fws_root.append(restart_wf_fw_id_mapping[restart_fws_root])

                    if restart_fws_leaf is None:
                        mapped_detour_fws_leaf.extend(restart_wf.leaf_fw_ids)
                    elif isinstance(restart_fws_leaf, (list, tuple)):
                        mapped_detour_fws_leaf.extend(
                            [restart_wf_fw_id_mapping[fw_id] for fw_id in restart_fws_leaf])
                    else:  # isinstance(restart_fws_leaf, int)
                        mapped_detour_fws_leaf.append(restart_wf_fw_id_mapping[restart_fws_leaf])

                    if restart_fws_root:
                        logger.debug("Mapped restart_wf root fw_ids {} onto newly created fw_ids {}".format(
                            restart_fws_root, mapped_detour_fws_root[-len(restart_fws_root):]))
                    if restart_fws_leaf:
                        logger.debug("Mapped restart_wf leaf fw_ids {} onto newly created fw_ids {}".format(
                            restart_fws_leaf, mapped_detour_fws_leaf[-len(restart_fws_leaf)]))

                    # apply updates to fw_spec
                    for fw in restart_wf.fws:
                        logger.debug("Insert restart counter '{}': {} into fw_id {}: '{}'.".format(
                            restart_counter, restart_count, fw.fw_id, fw.name))
                        set_nested_dict_value(
                            fw.spec, restart_counter, restart_count)

                    logger.debug(
                        "restart_wf:")
                    _log_nested_dict(logger.debug, restart_wf.as_dict())

                    # repeatedly append copy of this recover task:
                    recover_ft = self
                    logger.debug("subsequent recover_fw's task recover_ft:")
                    _log_nested_dict(logger.debug, recover_ft.as_dict())

                    # repeated recovery firework inherits the following specs:
                    # recover_fw_spec = {key: fw_spec[key] for key in fw_spec
                    #                   if key not in fw_spec_to_exclude}
                    recover_fw_spec = dict_merge({}, fw_spec,
                                                 merge_exclusions=fw_spec_to_exclude_dict)
                    set_nested_dict_value(recover_fw_spec, restart_counter, restart_count)
                    logger.debug("Insert restart counter '{}': {} into recover_fw.spec.".format(
                                 restart_counter, restart_count))
                    logger.debug("Propagate fw_spec = {} to subsequent "
                                 "recover_fw.".format(recover_fw_spec))

                    # merge insertions
                    #
                    #  + - - - - - - - - - - - - - - - - - - -+
                    #  ' detour_wf                            '
                    #  '                                      '
                    #  ' +------------------+     +---------+ '
                    #  ' | detour_wf roots  | --> | leaf(s) | ' ------+
                    #  ' +------------------+     +---------+ '       |
                    #  '                                      '       |
                    #  + - - - - - - - - - - - - - - - - - - -+       |
                    #                                                 |
                    #                                                 |
                    #                                                 |
                    #  + - - - - - - - - - - - - - - - - - - -+       |
                    #  ' restart_wf                           '       |
                    #  '                                      '       v
                    #  ' +------------------+     +---------+ '     +----------+
                    #  ' | restart_wf roots | --> | leaf(s) | ' --> | recovery |
                    #  ' +------------------+     +---------+ '     +----------+
                    #
                    # into one workflow and make repeated recovery fireworks
                    # dependent on all leaf fireworks of detour and restart:

                    if restart_wf is not None and detour_wf is not None:
                        detour_wf.append_wf(restart_wf, [])
                    elif restart_wf is not None:  # and detour wf is None
                        detour_wf = restart_wf

                    recover_fw = Firework(
                        recover_ft,
                        spec=recover_fw_spec,  # inherit this Firework's spec
                        name=repeated_recover_fw_name,
                        fw_id=self.consecutive_fw_id)
                    self.consecutive_fw_id -= 1
                    logger.info("Create repeated recover Firework {} with "
                                "id {} and specs {}".format(recover_fw.name,
                                                            recover_fw.fw_id,
                                                            recover_fw.spec))

                    recover_wf = Workflow([recover_fw])

                    detour_wf.append_wf(recover_wf, mapped_detour_fws_leaf)

                    # now we don't need the true 'mapped_detour_fws_leaf' anymore, recover_wf becomes the leaf
                    mapped_detour_fws_leaf = recover_wf.leaf_fw_ids
                else:
                    logger.warning(
                        "Maximum number of {} restarts reached. "
                        "No further restart.".format(max_restarts))
            else:
                logger.warning("No restart Fireworks appended.")

            if detour_wf is not None:
                self.write_files_prev(detour_wf, fw_spec, root_fw_ids=mapped_detour_fws_root)
                logger.debug(
                    "Workflow([*detour_wf.fws, *restart_wf.fws, recover_fw]):")
                _log_nested_dict(logger.debug, detour_wf.as_dict())

            if isinstance(addition_wf_dict, dict):
                addition_wf_base_spec = None
                if superpose_addition_on_parent_fw_spec:
                    if "spec" in prev_job_info:
                        addition_wf_base_spec = prev_job_info["spec"]
                    else:
                        logger.warning("Superposition of addition_wf's "
                                       "fw_spec onto parent's "
                                       "fw_spec desired, but not parent"
                                       "fw_spec recovered.")
                addition_wf, addition_wf_fw_id_mapping = self.appendable_wf_from_dict(
                    addition_wf_dict, base_spec=addition_wf_base_spec,
                    exclusions=fw_spec_to_exclude_dict)

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

        # end of ExitStack context
        output = {}
        if store_stdlog:
            stdlog_stream.flush()
            output['stdlog'] = stdlog_stream.getvalue()

        fw_action = FWAction()

        if stored_data:
            fw_action.stored_data = output

        if hasattr(fw_action, 'propagate') and propagate:
            fw_action.propagate = propagate

        if output_key:  # inject into fw_spec
            fw_action.mod_spec = [{dict_mod: {output_key: output}}]

        if addition_wf and apply_mod_spec_to_addition_wf:
            apply_mod_spec(addition_wf, fw_action, fw_ids=mapped_addition_fws_root)

        if detour_wf and apply_mod_spec_to_detour_wf:
            apply_mod_spec(detour_wf, fw_action, fw_ids=mapped_detour_fws_root)

        if addition_wf:  # for now, only one addition
            fw_action.additions = [addition_wf]
            fw_action.additions_root_fw_ids = [mapped_addition_fws_root]

        if detour_wf:
            # the leaf of a detour_wf is either another recover_fw or
            # the leaves of detour_wf_dict
            fw_action.detours = [detour_wf]
            fw_action.detours_root_fw_ids = [mapped_detour_fws_root]
            fw_action.detours_leaf_fw_ids = [mapped_detour_fws_leaf]

        return fw_action
