# coding: utf-8

from __future__ import unicode_literals

from abc import abstractmethod
from contextlib import ExitStack
from typing import List

import collections
import datetime
import getpass  # get system username for dtool metadata
import glob
import io
import json
import logging
import multiprocessing  # run task as child process to avoid side effects
import os
import traceback  # forward exception from child process to parent process

from ruamel.yaml import YAML

# from dtoolcore import DataSetCreator
import dtoolcore
import dtoolcore.utils
import dtool_create.dataset

from fireworks.fw_config import FW_LOGGING_FORMAT

from fireworks.core.firework import FiretaskBase, FWAction
from fireworks.utilities.dict_mods import get_nested_dict_value
from fireworks.utilities.fw_serializers import ENCODING_PARAMS


__author__ = 'Johannes Laurin Hoermann'
__copyright__ = 'Copyright 2020, IMTEK Simulation, University of Freiburg'
__email__ = 'johannes.hoermann@imtek.uni-freiburg.de, johannes.laurin@gmail.com'
__date__ = 'Apr 27, 2020'

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


# adapted from https://github.com/jic-dtool/dtool-create/blob/0a772aa5157523a7219963803293d4e521bc1aa2/dtool_create/dataset.py#L40
def _get_readme_template(fpath=None):
    if fpath is None:
        fpath = dtoolcore.utils.get_config_value(
            "DTOOL_README_TEMPLATE_FPATH",
        )
    if fpath is None:
        fpath = dtool_create.dataset.README_TEMPLATE_FPATH

    with open(fpath) as fh:
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


class LoggingContext():
    """Modifies logging within context."""
    def __init__(self, logger=None, handler=None, level=None, close=False):
        self.logger = logger
        self.level = level
        self.handler = handler
        self.close = close

    def __enter__(self):
        """Prepare log output streams."""
        if self.logger is None:
            # temporarily modify root logger
            self.logger = logging.getLogger('')
        if self.level is not None:
            self.old_level = self.logger.level
            self.logger.setLevel(self.level)
        if self.handler:
            self.logger.addHandler(self.handler)

    def __exit__(self, et, ev, tb):
        if self.level is not None:
            self.logger.setLevel(self.old_level)
        if self.handler:
            self.logger.removeHandler(self.handler)
        if self.handler and self.close:
            self.handler.close()
        # implicit return of None => don't swallow exceptions


class TemporaryOSEnviron:
    """Preserve original os.environ context manager."""

    def __init__(self, env=None):
        """env is a flat dict to be inserted into os.environ."""
        self._insertions = env

    def __enter__(self):
        """Store backup of current os.environ."""
        logger = logging.getLogger(__name__)
        logger.debug("Backed-up os.environ:")
        _log_nested_dict(logger.debug, os.environ)
        self._original_environ = os.environ.copy()

        if self._insertions:
            for k, v in self._insertions.items():
                logger.debug("Inject env var '{}' = '{}'".format(k, v))
                os.environ[k] = str(v)

        logger.debug("Initial modified os.environ:")
        _log_nested_dict(logger.debug, os.environ)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Restore backed up os.environ."""
        logger = logging.getLogger(__name__)
        os.environ = self._original_environ
        logger.debug("Recovered os.environ:")
        _log_nested_dict(logger.debug, os.environ)


# in order to make sure any modifications to the environment within dtool
# won't pollute Fireworks process environmnet, run task in separate process
# https://stackoverflow.com/questions/19924104/python-multiprocessing-handling-child-errors-in-parent
class Process(multiprocessing.Process):
    """
    Class which returns child Exceptions to Parent.
    https://stackoverflow.com/a/33599967/4992248
    """

    def __init__(self, *args, **kwargs):
        multiprocessing.Process.__init__(self, *args, **kwargs)
        self._parent_conn, self._child_conn = multiprocessing.Pipe()
        self._exception = None

    def run(self):
        try:
            multiprocessing.Process.run(self)
            self._child_conn.send(None)
        except Exception as e:
            tb = traceback.format_exc()
            self._child_conn.send((e, tb))
            raise e  # You can still rise this exception if you need to

    @property
    def exception(self):
        if self._parent_conn.poll():
            self._exception = self._parent_conn.recv()
        return self._exception


class DtoolTask(FiretaskBase):
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
            properties into output key, see fireworks.utilities.dict_mods
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
    required_params: List[str] = []
    optional_params = [
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
        ...

    def run_task(self, fw_spec):
        """Spawn child process to assure my environment stays untouched."""
        q = multiprocessing.Queue()
        p = Process(target=self._run_task_as_child_process, args=(q, fw_spec,))

        p.start()

        # wait for child to queue fw_action object and
        # check whether child raises exception
        while q.empty():
            # if child raises exception, then it has terminated
            # before queueing any fw_action object
            if p.exception:
                error, p_traceback = p.exception
                raise ChildProcessError(p_traceback)
        # this loop will deadlock for any child that never raises
        # an exception and does not queue anything

        # queue only used for one transfer of
        # return fw_action, should thus never deadlock.
        fw_action = q.get()
        # if we reach this line without the child
        # queueing anything, then process will deadlock.
        p.join()
        return fw_action

    def _run_task_as_child_process(self, q, fw_spec):
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

    This tast extends the basic DtoolTask parameters by

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
        - name (str): Name of dataset, default: "dataset".
        - paths (list/str): Either list of paths or a glob pattern string.
            Per default, all content of 'directory'.
        - source_dataset_uri (str): A derived dataset will be created if
            specified. Default: None.

    The dataset's README.yml file is a successive merge of the README template,
    static metadata and dynamic metadata, ordered by increasing precedence in
    the case of conflicting fields.

    Fields 'creator_username', 'dtool_readme_template_path', 'name', and
    'source_dataset_uri' may also be a dict of format
    { 'key': 'some->nested->fw_spec->key' } for looking up value within
    fw_spec instead.
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
        "source_dataset_uri"]

    def _run_task_internal(self, fw_spec):
        logger = logging.getLogger(__name__)

        name = self.get(
            "name", "dataset")
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
        if source_dataset_uri is not None:
            logger.info("Derive from '{}'.".format(source_dataset_uri))

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

        if source_dataset_uri is not None:
            parsed_source_dataset_uri = dtoolcore.utils.generous_parse_uri(
                source_dataset_uri)
            logger.info("Derive new dataset from '{}'.".format(
                parsed_source_dataset_uri))
            source_dataset = dtoolcore.DataSet.from_uri(source_dataset_uri)
            logger.info(
                "Source dataset has name '{}', uri '{}', and uuid '{}'."
                .format(source_dataset.name, source_dataset.uri, source_dataset.uuid))
            proto_dataset = dtoolcore.create_derived_proto_dataset(
                name=name,
                base_uri=dtoolcore.utils.urlunparse(parsed_base_uri),
                source_dataset=source_dataset,
                readme_content=readme,
                creator_username=creator_username)
        else:
            proto_dataset = dtoolcore.create_proto_dataset(
                name=name,
                base_uri=dtoolcore.utils.urlunparse(parsed_base_uri),
                readme_content=readme,
                creator_username=creator_username)

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

    This tast extends the basic DtoolTask parameters by

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

    This tast extends the basic DtoolTask parameters by

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
