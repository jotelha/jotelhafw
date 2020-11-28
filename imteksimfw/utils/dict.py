# coding: utf-8
#
# dict.py
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
"""Utility functions for nested dicts and lists."""

import collections
import logging

from fireworks.utilities.dict_mods import get_nested_dict_value, apply_mod
from imteksimfw.utils.logging import _log_nested_dict

__author__ = 'Johannes Laurin Hoermann'
__copyright__ = 'Copyright 2020, IMTEK Simulation, University of Freiburg'
__email__ = 'johannes.hoermann@imtek.uni-freiburg.de, johannes.laurin@gmail.com'
__date__ = 'August 27, 2020'


# from https://gist.github.com/angstwad/bf22d1822c38a92ec0a9
def simple_dict_merge(dct, merge_dct, add_keys=True):
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


# from https://gist.github.com/angstwad/bf22d1822c38a92ec0a9
def dict_merge(dct, merge_dct, add_keys=True,
               exclusions={}, merge_exclusions={}):
    """ Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
    updating only top-level keys, dict_merge recurses down into dicts nested
    to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
    ``dct``.

    This version will return a copy of the dictionary and leave the original
    arguments untouched.

    The optional argument ``add_keys``, determines whether keys which are
    present in ``merge_dct`` but not ``dct`` should be included in the
    new dict.

    Args:
        dct (dict) onto which the merge is executed
        merge_dct (dict): dct merged into dct
        add_keys (bool): whether to add new keys
        exclusions (dict): any such key found within 'dct' will be removed.
                               It can, however, be reintroduced if present in
                               'merge_dct' and 'add_keys' is set.
        merge_exclusions (dict): any such key found within 'merge_dct'
                                     will be ignored. Such keys allready
                                     present within 'dct' are not touched.

    Returns:
        dict: updated dict
    """
    logger = logging.getLogger(__name__)

    merge_dct = merge_dct.copy()
    dct = dct.copy()

    logger.debug("Merge 'merge_dct'...")
    _log_nested_dict(logger.debug, merge_dct)
    logger.debug("... into 'dct'...")
    _log_nested_dict(logger.debug, dct)
    logger.debug("... with 'exclusions'...")
    _log_nested_dict(logger.debug, exclusions)
    logger.debug("... and 'merge_exclusions'...")
    _log_nested_dict(logger.debug, merge_exclusions)

    # remove all entries marked as exclusions from dct
    for k in exclusions:
        if (k in dct) and (exclusions[k] is True):
            logger.debug("Key '{}' excluded from dct.".format(k))
            del dct[k]

    if not add_keys:
        merge_dct = {
            k: merge_dct[k] for k in set(dct).intersection(set(merge_dct))}
        logger.debug(
            "Not merging keys only in 'merge_dict', only merging {}.".format(
                merge_dct.keys()))

    # Add empty fields to merge_dct for all keys in dct but not in merge_dct.
    # This makes sure the union of all keys from dct stripped off exclusions
    # and merge_dct not yet stripped off merge_exclusions is treated in the
    # following.
    for k in dct.keys():
        if isinstance(dct[k], dict) and k not in merge_dct:
            merge_dct[k] = {}

    for k, v in merge_dct.items():
        # all truly excluded keys allready removed from dct, thus an existing
        # entry in exclusions means that some more specific nested key
        # is excluded
        if k in exclusions:
            lower_level_exclusions = exclusions[k]
            logger.debug("Key '{}' included in dct, but exclusions exist for nested keys.".format(k))
        else:
            lower_level_exclusions = {}
            logger.debug("Key '{}' included in dct.".format(k))

        # only treat nested nested dicts for now
        # TODO: treat also nested lists
        if (k in dct and isinstance(dct[k], dict)
                and isinstance(v, collections.Mapping)):
            if k not in merge_exclusions:  # no exception rule for this field
                logger.debug("Key '{}' included in merge.".format(k))
                dct[k] = dict_merge(dct[k], v, add_keys=add_keys,
                                    exclusions=lower_level_exclusions)
            elif merge_exclusions[k] is not True:  # exclusion rule for nested fields
                logger.debug("Key '{}' included in merge, but exclusions exist for nested keys.".format(k))
                dct[k] = dict_merge(dct[k], v, add_keys=add_keys,
                                    exclusions=lower_level_exclusions,
                                    merge_exclusions=merge_exclusions[k])
            else:
                logger.debug("Key '{}' excluded from merge.".format(k))
        else:
            if k not in merge_exclusions:  # no exception rule for this field
                logger.debug("Key '{}' included in merge.".format(k))
                dct[k] = v
            else:
                logger.debug("Key '{}' excluded from merge.".format(k))

    return dct


def _make_marker(d):
    """Mark everything for comparison."""
    if isinstance(d, list):
        return [_make_marker(e) for e in d]
    elif isinstance(d, dict):
        return {k: _make_marker(v) for k, v in d.items()}
    else:
        return True

def _compare(source, target, marker):
    """Compare source and target partially, as marked by marker."""
    logger = logging.getLogger(__name__)
    if isinstance(marker, dict):
        for k, v in marker.items():
            if k not in source:
                logger.error("{} not in source '{}'.".format(k, source))
                return False
            if k not in target:
                logger.error("{} not in target '{}'.".format(k, source))
                return False

            logger.debug("Descending into sub-tree '{}' of '{}'.".format(
                source[k], source))
            # descend
            if not _compare(source[k], target[k], v):
                return False  # one failed comparison suffices

    elif isinstance(marker, list):  # source, target and marker must have same length
        logger.debug("Branching into element wise sub-trees of '{}'.".format(
            source))
        for s, t, m in zip(source, target, marker):
            if not _compare(s, t, m):
                return False  # one failed comparison suffices
    else:  # arrived at leaf, comparison desired?
        if marker is not False:  # yes
            logger.debug("Comparing '{}' == '{}' -> {}.".format(
                source, target, source == target))
            return source == target

    # comparison either not desired or successfull for all elements
    return True


def compare(source, target, marker=None):
    """Compare source and target partially, as marked by marker. If marker is None, then compare everything."""
    if not marker:
        marker = _make_marker(source)
    return _compare(source, target, marker)


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


# we apply update_spec and mod_spec here a priori because additions and detours
# won't be touched by the default mechanism in fireworks.core.firework
def apply_mod_spec(wf, action, fw_ids=None):
    """Update the spec of the children FireWorks using DictMod language."""
    fw_ids = fw_ids if fw_ids else wf.leaf_fw_ids
    updated_ids = []

    if action.update_spec and action.propagate:
        # Traverse whole sub-workflow down to leaves.
        visited_cfid = set()  # avoid double-updating for diamond deps

        def recursive_update_spec(fw_ids):
            for cfid in fw_ids:
                if cfid not in visited_cfid:
                    visited_cfid.add(cfid)
                    wf.id_fw[cfid].spec.update(action.update_spec)
                    updated_ids.append(cfid)
                    recursive_update_spec(wf.links[cfid])

        recursive_update_spec(fw_ids)
    elif action.update_spec:
        # Update only direct children.
        for cfid in fw_ids:
            wf.id_fw[cfid].spec.update(action.update_spec)
            updated_ids.append(cfid)

    if action.mod_spec and action.propagate:
        visited_cfid = set()

        def recursive_mod_spec(fw_ids):
            for cfid in fw_ids:
                if cfid not in visited_cfid:
                    visited_cfid.add(cfid)
                    for mod in action.mod_spec:
                        apply_mod(mod, wf.id_fw[cfid].spec)
                    updated_ids.append(cfid)
                    recursive_mod_spec(cfid)

        recursive_mod_spec(fw_ids)
    elif action.mod_spec:
        for cfid in fw_ids:
            for mod in action.mod_spec:
                apply_mod(mod, wf.id_fw[cfid].spec)
            updated_ids.append(cfid)

    return updated_ids
