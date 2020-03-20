#!/usr/bin/env python3
"""FireWorksRocketLauncherManager configuration"""

import os
import logging
import monty.serialization  # for reading config files

# configuration handling modeled following
# https://github.com/materialsproject/fireworks/blob/master/fireworks/fw_config.py

FWRLM_CONFIG_FILE_DIR = os.path.join(os.path.expanduser('~'), ".fireworks")
FWRLM_CONFIG_FILE_NAME = 'FWRLM_config.yaml'
FWRLM_CONFIG_FILE_ENV_VAR = 'FWRLM_CONFIG_FILE'

FW_CONFIG_PREFIX = os.path.join(os.path.expanduser('~'), ".fireworks")
FW_CONFIG_FILE_NAME = "FW_config.yaml"
FW_AUTH_FILE_NAME = "fireworks_mongodb_auth.yaml"

LAUNCHPAD_LOC = os.path.join(os.path.expanduser('~'), "fw_launchpar")
LOGDIR_LOC = os.path.join(os.path.expanduser('~'), "fw_logdir")

# allow multiple rlaunch processes
MULTI_RLAUNCH_NTASKS = 0
OMP_NUM_THREADS = 1

# gui settings
FWGUI_PORT = 19886

# mongodb and ssh tunnel settings
MONGODB_HOST = 'localhost'
MONGODB_PORT_REMOTE = 27017
MONGODB_PORT_LOCAL = 27037
FIREWORKS_DB = 'fireworks'
FIREWORKS_USER = 'fireworks'
FIREWORKS_PWD = 'fireworks'
SSH_HOST = '132.230.102.164'
SSH_USER = 'sshclient'
SSH_TUNNEL = False
SSH_KEY = os.path.join(os.path.expanduser('~'), ".ssh", "id_rsa")
USE_RSTUNNEL = True
RSTUNNEL_CONFIG = None # path to rstunnel config file

# run daemon to periodically check offline runs
RECOVER_OFFLINE = True

# MACHINE-specfific settings
MACHINE = "JUWELS"

RLAUNCH_FWORKER_FILE = None
# if not set explicitl, then stick to automatic convention
# "${FW_CONFIG_PREFIX}/${MACHINE:lowercase}_noqueue_worker.yaml"

QLAUNCH_FWORKER_FILE = None
# if not set explicitl, then stick to automatic convention
# "${FW_CONFIG_PREFIX}/${MACHINE:lowercase}_queue_offline_worker.yaml"

QADAPTER_FILE = None
# if not set explicitl, then stick to automatic convention
# "${FW_CONFIG_PREFIX}/${MACHINE:lowercase}_{SCHEDULER_lowercase}_qadapter_offline.yaml"

SCHEDULER = 'SLURM'


def override_user_settings():
    logger = logging.getLogger(__name__)
    module_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(module_dir)  # FW root dir

    config_paths = []

    test_paths = [
        os.getcwd(),
        os.path.join(os.path.expanduser('~'), ".fireworks"),
        root_dir,
    ]

    for p in test_paths:
        fp = os.path.join(p, FWRLM_CONFIG_FILE_NAME)
        if fp not in config_paths and os.path.exists(fp):
            config_paths.append(fp)

    if FWRLM_CONFIG_FILE_ENV_VAR in os.environ \
        and os.environ[FWRLM_CONFIG_FILE_ENV_VAR] not in config_paths:
        config_paths.append(os.environ[FWRLM_CONFIG_FILE_ENV_VAR])


    if len(config_paths) > 1:
        logger.warn("Found many potential paths for {}: {}"
            .format(FWRLM_CONFIG_FILE_NAME, config_paths))
        logger.warn("Choosing as default: {}"
            .format(config_paths[0]))

    if len(config_paths) > 0 and os.path.exists(config_paths[0]):
        overrides = monty.serialization.loadfn(config_paths[0])
        for key, v in overrides.items():
            if key not in globals():
                raise ValueError(
                    'Invalid FWRLM_config file has unknown parameter: {}'
                        .format(key))
            else:
                logger.info("Set key : value pair '{}' : '{}'"
                    .format(key, v))
                globals()[key] = v

def config_to_dict():
    d = {}
    for k, v in globals().items():
        if k.upper() == k:
            d[k] = v
    return d

def config_keys_to_list():
    l = []
    for k in globals().keys():
        if k.upper() == k:
            l.append(k)
    return l


def write_config(path=None):
    path = os.path.join(FWRLM_CONFIG_FILE_DIR, FWRLM_CONFIG_FILE_NAME) if path is None else path
    monty.serialization.dumpfn(config_to_dict(), path)

def write_config_keys(path):
    monty.serialization.dumpfn(config_keys_to_list(), path)

override_user_settings()
