# coding: utf-8
#
# storage_tasks.py
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
"""Tasks that push to and pull from various storage platforms."""

import io
import json
import logging
import os
import pymongo

from ruamel.yaml import YAML
from contextlib import ExitStack

from fireworks.core.firework import FWAction, FiretaskBase
from fireworks.utilities.filepad import FilePad
from fireworks.utilities.dict_mods import get_nested_dict_value, arrow_to_dot

from imteksimfw.utils.logging import LoggingContext, _log_nested_dict
from imteksimfw.utils.dict import dict_merge

from fireworks.fw_config import FW_LOGGING_FORMAT
from fireworks.utilities.fw_serializers import ENCODING_PARAMS

__author__ = 'Johannes Laurin Hoermann'
__copyright__ = 'Copyright 2020, IMTEK Simulation, University of Freiburg'
__email__ = 'johannes.hoermann@imtek.uni-freiburg.de, johannes.laurin@gmail.com'
__date__ = 'August 27, 2020'

DEFAULT_FORMATTER = logging.Formatter(FW_LOGGING_FORMAT)


def get_fpad(fpad_file):
    if fpad_file:
        return FilePad.from_db_file(fpad_file)
    else:
        return FilePad.auto_load()


class GetObjectFromFilepadTask(FiretaskBase):
    """
    A Firetask to query an object from the filepad and write them to specified
    directory (current working directory if not specified).

    Required params:
        - query (dict or str): mongo db query identifying files to fetch.
          Same as within fireworks.utils.dict_mods, use '->' in dict keys
          for querying nested documents, instead of MongoDB '.' (dot) seperator.
          Do use '.' and NOT '->' within the 'sort_key' field.

    Optional params:
        - sort_key (str): sort key, don't sort per default.
        - sort_direction (int): sort direction, default 'pymongo.DESCENDING'.
            Query can yield multiple objects, only first object in results
            pulled.
        - fizzle_empty_result (bool): fizzle if no file found, default: True
        - filepad_file (str): path to the filepad db config file
        - dest_dir (str): destination directory, default is the current working
            directory.
        - new_file_name (str): if provided, the retrieved file will be
            renamed.
        - metadata_file (str): default: None. Write in YAML-formatted metadata
            of queried object to this file within dest_dir.
        - metadata_fp_source_key (str): if specified, only use nested subset of
            metadata found for filepad object. Default: None or "" uses all.
        - metadata_fw_dest_key (str): forward filepad metadata of queried object
            to this fw_spec field of subsequent Fireworks via FWAction.
            Default: None, metadata not kept in pipeline. Empty string
            points to root.
        - metadata_fw_source_key (str): merge metadata from Filepad object with
            metadata within this fw_spec key before forwarding.
        - fw_supersedes_fp: if True, metadata from metadata_fw_source_key
            supersedes metadata from metadata_fp_source_key. Otherwise, latter
            supersedes former. Default: False
        - metadata_fp_exclusions (dict): mark exclusions from metadata forwarded
            into pipeline with nested dict {'excluded': {'field': True}}.
        - metadata_fw_exclusions (dict): mark exclusions from
            metadata_fw_source_key merged with metadata_fp_source_key.

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
    """
    _fw_name = 'GetObjectsFromFilepadTask'
    required_params = ["query"]
    optional_params = [
        "filepad_file",

        "fizzle_empty_result",
        "sort_direction",
        "sort_key",

        "dest_dir",
        "new_file_name",

        "metadata_file",

        "metadata_fp_source_key",
        "metadata_fw_source_key",
        "metadata_fw_dest_key",
        "fw_supersedes_fp",
        "metadata_fp_exclusions",
        "metadata_fw_exclusions",

        "stored_data",
        "output",
        "dict_mod",
        "propagate",
        "stdlog_file",
        "store_stdlog",
        "loglevel"
        ]

    def run_task(self, fw_spec):
        fpad = get_fpad(self.get("filepad_file", None))
        dest_dir = self.get("dest_dir", os.path.abspath(os.path.curdir))
        new_file_name = self.get("new_file_name", None)
        query = self.get("query", {})
        sort_key = self.get("sort_key", None)
        sort_direction = self.get("sort_direction", pymongo.DESCENDING)
        fizzle_empty_result = self.get("fizzle_empty_result", True)
        metadata_file = self.get("metadata_file", None)

        metadata_fp_source_key = self.get("metadata_fp_source_key", None)
        metadata_fw_source_key = self.get("metadata_fw_source_key", None)
        metadata_fw_dest_key = self.get("metadata_fw_dest_key", None)
        fw_supersedes_fp = self.get("fw_supersedes_fp", False)
        metadata_fp_exclusions = self.get("metadata_fp_exclusions", {})
        metadata_fw_exclusions = self.get("metadata_fw_exclusions", {})

        # generic parameters
        stored_data = self.get('stored_data', False)
        output_key = self.get('output', None)
        dict_mod = self.get('dict_mod', '_set')
        propagate = self.get('propagate', False)

        stdlog_file = self.get('stdlog_file', '{}.log'.format(self._fw_name))
        store_stdlog = self.get('store_stdlog', False)
        loglevel = self.get('loglevel', logging.INFO)

        mod_spec = []
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

            if isinstance(query, dict):
                query = arrow_to_dot(query)
            # else assumed str

            if sort_key is None:
                cursor = fpad.filepad.find(query)
            else:
                cursor = fpad.filepad.find(query).sort(sort_key, sort_direction)

            try:
                d = next(cursor)
            except StopIteration:
                if fizzle_empty_result:
                    raise ValueError("Query yielded empty result! (query: {:s})".format(
                        json.dumps(query)))
                else:
                    logger.info("Query yielded empty result! (query: {:s})".format(json.dumps(query)))
            else:
                file_contents, doc = fpad._get_file_contents(d)
                logger.debug("Metadata of queried object:")
                _log_nested_dict(logger.debug, doc)

                file_name = new_file_name if new_file_name else doc["original_file_name"]
                with open(os.path.join(dest_dir, file_name), "wb") as f:
                    f.write(file_contents)

                if metadata_file:
                    metadata_file_path = os.path.join(dest_dir, metadata_file)
                    logger.debug("Writing metadata of queried object to '{}'.".format(metadata_file_path))
                    with open(metadata_file_path, "w") as f:
                        yaml = YAML()
                        yaml.dump(doc, f)

                if metadata_fw_dest_key:
                    if metadata_fp_source_key and metadata_fp_source_key != "":
                        fp_metadata = get_nested_dict_value(doc, metadata_fp_source_key)
                        logger.debug(
                            "Forwarding '{}' subset of filepad object metadata:".format(metadata_fp_source_key))
                        _log_nested_dict(logger.debug, fp_metadata)
                    else:
                        fp_metadata = doc
                        logger.debug("Forwarding filepad object metadata.")

                    if metadata_fw_source_key and metadata_fw_source_key == "":
                        fw_metadata = fw_spec
                        logger.debug("Merging with fw_spec.")
                    elif metadata_fw_source_key:
                        fw_metadata = get_nested_dict_value(fw_spec, metadata_fw_source_key)
                        logger.debug("Merging with '{}' subset of fw_spec:".format(metadata_fw_source_key))
                        _log_nested_dict(logger.debug, fw_metadata)
                    else:
                        fw_metadata = {}

                    if fw_supersedes_fp:
                        metadata = dict_merge(fp_metadata, fw_metadata,
                                              exclusions=metadata_fp_exclusions,
                                              merge_exclusions=metadata_fw_exclusions)
                    else:
                        metadata = dict_merge(fw_metadata, fp_metadata,
                                              exclusions=metadata_fw_exclusions,
                                              merge_exclusions=metadata_fp_exclusions)

                    logger.debug("Forwarding merge into '{}':".format(metadata_fw_dest_key))
                    _log_nested_dict(logger.debug, metadata)

                    mod_spec.append({dict_mod: {metadata_fw_dest_key: metadata}})

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
            mod_spec.append({dict_mod: {output_key: output}})

        if len(mod_spec) > 0:
            fw_action.mod_spec = mod_spec

        return fw_action
