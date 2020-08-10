#!/usr/bin/env python
#
# cmd_tasks.py
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

import collections
import glob
import io
import json
import logging
import os
import shutil

from collections.abc import Iterable
from contextlib import ExitStack

from fireworks.fw_config import FW_LOGGING_FORMAT
from fireworks.utilities.fw_serializers import ENCODING_PARAMS
from fireworks.utilities.dict_mods import get_nested_dict_value, set_nested_dict_value
from fireworks.core.firework import FWAction, FiretaskBase, Firework, Workflow

from imteksimfw.fireworks.utilities.logging import LoggingContext

__author__ = 'Johannes Laurin Hoermann'
__copyright__ = 'Copyright 2020, IMTEK Simulation, University of Freiburg'
__email__ = 'johannes.hoermann@imtek.uni-freiburg.de, johannes.laurin@gmail.com'
__date__ = 'August 10, 2020'

DEFAULT_FORMATTER = logging.Formatter(FW_LOGGING_FORMAT)

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

    Optional parameters:
        - recover (bool): Pull launchdir from previous FireWork if True.
            Otherwise assume output files to have been produced in same Firework.
            Default: True
        - restart_wf (dict): Workflow or single FireWork to append only if
            restart file present (parent failed). Task will not append anything
            if None. Default: None
        - detour_wf (dict): Workflow or single FireWork to always append as
            a detour, independent on parent's success (i.e. post-processing).
            Task will not append anything if None. Default: None
        - addition_wf (dict): Workflow or single FireWork to always append as
            an addition, independent on parent's success (i.e. storage).
            Default: None.

        - default_restart_file (str): Name of restart file. Most recent restart
            file found in fizzled parent via 'restart_file_glob_patterns'
            will be copied to new launchdir under this name. Default: None
        - fizzle_on_no_restart_file (bool): Default: True
        - fw_spec_to_exclude ([str]): All insertions will inherit the current
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

        - store_stdlog (bool, default: False): insert log output into database
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
    _fw_name = "RecoverLammpsTask"
    required_params = []
    optional_params = [
        "recover",
        "restart_fw",
        "detour_wf",
        "addition_wf",

        "default_restart_file",
        "fizzle_on_no_restart_file",
        "fw_spec_to_exclude",
        "ignore_errors",
        "max_restarts",
        "other_glob_patterns",
        "repeated_recover_fw_name",
        "restart_counter",
        "restart_file_glob_patterns",

        "stdlog_file",
        "store_stdlog",
        "loglevel"]

    def appendable_wf_from_dict(self, obj_dict, fw_spec):
        """Creates Workflow from a Workflow or single FireWork dict description.

        Sets _files_prev in roots of new workflow."""
        logger = logging.getLogger(__name__)

        # if detour_fw given, append in any case:
        if isinstance(obj_dict, dict):
            # in case of single Fireworks:
            if "spec" in obj_dict:
                # append firework (defined as dict):
                fw = Firework.from_dict(obj_dict)
                fw.fw_id = self.consecutive_fw_id
                self.consecutive_fw_id -= 1
            wf = Workflow([fw])
        else:   # if no single fw, then wf
            wf = Workflow.from_dict(obj_dict)
            # do we have to reassign fw_ids?

        # modeled to match original snippet from fireworks.core.rocket:

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
        if fw_spec.get("_files_out"):
            logger.info("Current FireWork's '_files_out': {}".format(
                        fw_spec.get("_files_out")))

            files_prev = {}

            for k, v in fw_spec.get("_files_out").items():
                files = glob.glob(os.path.join(os.curdir, v))
                if files:
                    logger.info("This Firework provides {}: {}".format(
                                k, files), " within _files_out.")
                    filepath = sorted(files)[-1]
                    logger.info("{}: '{}' provided as '_files_prev'".format(
                                k, filepath), " to subsequent FireWorks.")
                    files_prev[k] = filepath

            # get roots of insertion wf and assign _files_prev to them
            root_fws = [fw for fw in wf.fws if fw.fw_id in wf.root_wf_ids]
            for root_fw in root_fws:
                root_fw.spec["_files_prev"] = files_prev

        return wf


    def run_task(self, fw_spec):
        self.consecutive_fw_id = -1  # quite an ugly necessity

        # get fw_spec entries or their default values:
        recover = self.get('recover', True)
        restart_wf_dict = self.get('restart_wf', None)
        detour_wf_dict = self.get('detour_wf', None)
        addition_wf_dict = self.get('addition_wf', None)

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

        fw_spec_to_exclude = self.get('fw_spec_to_exclude',
                                      [
                                        '_job_info',
                                        '_fw_env',
                                        '_files_prev',
                                        '_fizzled_parents',
                                      ])

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

            if (len(restart_file_dests) > 1):
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

            # check whether a previous firework handed down information
            prev_job_info = None
            path_prefix = None
            # pull from intentionally passed job info:
            if recover and ('_job_info' in fw_spec):
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
            elif recover and ('_fizzled_parents' in fw_spec):
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
            elif recover:  # no info about previous (fizzled or other) jobs
                logger.info(
                    'No information about previous (fizzled or other) jobs available.')
                # completed successfully?
            else:
                # assume all output files in current directory
                path_prefix = os.getcwd()
                logger.info('Work on own launch dir.')

            # only recover, if prev_job info not None

            # find other files to forward:
            file_list = []
            # current_restart_file = None

            # if prev_job_info is not None:
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
                    restart_file_list.append(
                        (sorted_restart_file_matches[-1], dest))
                elif len(restart_file_list) == 1:
                    logger.info("One restart file {} for glob "
                                "pattern {}".format(
                                    restart_file_matches[0]))
                    restart_file_list.append(
                        (restart_file_matches[0], dest))
                else:
                    logger.info("No restart file!")
                    if fizzle_on_no_restart_file:
                         raise ValueError(
                            "No restart file in {} for glob pattern {}".format(
                                path_prefix, glob_pattern))

            # distinguish between FireWorks and Workflows by top-level keys
            # fw: ['spec', 'fw_id', 'created_on', 'updated_on', 'name']
            # wf: ['fws', 'links', 'name', 'metadata', 'updated_on', 'created_on']
            detour_wf = None
            addition_wf = None

            # if detour_fw given, append in any case:
            if isinstance(detour_wf_dict, dict):
                detour_wf = self.appendable_wf_from_dict(detour_wf_dict, fw_spec)

            if detour_wf is not None:
                logger.debug(
                    "detour_wf:")
                _log_nested_dict(logger.debug, detour_wf.as_dict())

            # append restart fireworks if restart file exists
            if len(restart_file_list) > 0 and restart_wf_dict:
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

                # if still none found, assume it's the "0th"
                if restart_count is None:
                    restart_count = 0
                else:  # make sure above's queried value is an integer
                    restart_count = int(restart_count) + 1

                if restart_count < max_restarts + 1:
                    logger.info(
                        "This is #{:d} of at most {:d} restarts.".format(
                            restart_count+1, max_restarts))

                    restart_wf = self.appendable_wf_from_dict(restart_wf_dict,
                                                              fw_spec)

                    # apply updates to fw_spec
                    for fws in restart_wf.fws:
                        set_nested_dict_value(
                            fws.spec, restart_counter, restart_count)

                    logger.debug(
                        "restart_wf:")
                    _log_nested_dict(logger.debug, restart_wf.as_dict())


                    # repeatedly append copy of this recover task:
                    recover_ft = self

                    # repeated recovery firework inherits the following specs:
                    recover_fw_spec = {key: fw_spec[key] for key in fw_spec
                                       if key not in fw_spec_to_exclude}

                    # make repeated recovery fireworks dependent on all
                    # other leaf fireworks (i.e. detour and restart):
                    recover_fw_parents = []
                    if restart_wf is not None:
                        recover_fw_parents.extend(restart_wf.leaf_fw_ids)
                    if detour_wf is not None:
                        recover_fw_parents.extend(detour_wf.leaf_fw_ids)

                    recover_fw = Firework(
                        recover_ft,
                        spec=recover_fw_spec,  # inherit this Firework's spec
                        name=repeated_recover_fw_name,
                        parents=recover_fw_parents,
                        fw_id=self.consecutive_fw_id)
                    self.consecutive_fw_id -= 1
                    logger.info("Create repeated recover Firework {} with "
                                "id {} and specs {}".format(recover_fw.name,
                                                            recover_fw.fw_id,
                                                            recover_fw.spec))

                    # merging insertions
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
                    # into one workflow

                    fws = []
                    if restart_wf is not None:
                        fws.extend(restart_wf.fws)
                    if detour_wf is not None:
                        fws.extend(detour_wf.fws)

                    detour_wf = Workflow(
                        [*fws, recover_fw])

                    logger.debug(
                        "Workflow([*detour_wf.fws, *restart_wf.fws, recover_fw]):")
                    _log_nested_dict(logger.debug, detour_wf.as_dict())
                else:
                    logger.info(
                        "Maximum number of {} restarts reached. ".format(
                        max_restarts), "No further restart.")
            else:
                logger.warning("No restart file, no restart Fireworks appended.")

            # if detour_fw given, append in any case:
            if isinstance(addition_wf_dict, dict):
                addition_wf = self.appendable_wf_from_dict(addition_wf_dict,
                                                           fw_spec)

            return FWAction(
                additions=addition_wf,
                detours=detour_wf)
