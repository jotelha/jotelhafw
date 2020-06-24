# Curstom FireWorks extensions

Johannes Hörmann, johannes.hoermann@imtek.uni-freiburg.de, Mar 2020

# Quick start

Install the official FireWorks package, i.e. by `pip install fireworks`,
(https://github.com/materialsproject/fireworks) and subsequently make this
package available to your FireWorks environment, i.e. by
`pip install imteksimfw`.

## Custom FireTasks quick start

To use custom FireTasks within `imteksimfw`, append

    ADD_USER_PACKAGES:
      - imteksimfw.fireworks.user_objects.firetasks

to your `~/.fireworks/FW_config.yaml`.

## FWRLM quick start: the FireWorks RocketLauncher Mananger

In order to set up a well-defined FireWorks environment on an HPC system,
a multitude of configuration files (`FW_config.yaml`,
database authentication file, worker files, queue adapter files,
submit script templates) as well as a few background services (depending on
the HPC system that might include an ssh connection to the data base,
a simple rocket launcher for data transfer FireWorks on a login node,
queue submission of computationally expensive FireWorks,
a loop for recovering submitted offline FireWorks regularly) are required.

`fwrlm` provides a simple, standardized interface for

- quickly generating configuration file sets from templates
- starting and stopping the above mentioned persistent background services

### Configuring FWRLM & FireWorks

To get started, pick your favorite configuration file template
`FWRLM_config.yaml.SAMPLE_MACHINE` from within `imteksimfw/fireworks/examples`
and copy it to your home directory as `FWRLM_config.yaml`. It might look like
this:

```
MACHINE:   JUWELS
SCHEDULER: SLURM

FW_CONFIG_SKEL_PREFIX:     /path/to/imteksimfw/fireworks/examples/fw_config
FW_CONFIG_TEMPLATE_PREFIX: /path/to/imteksimfw/fireworks/templates/fw_config

FW_CONFIG_PREFIX:     /path/to/your/home/directory/.fireworks
FW_CONFIG_FILE_NAME:  FW_config.yaml
FW_AUTH_FILE_NAME:    fireworks_mongodb_auth.yaml
LAUNCHPAD_LOC:        /path/to/your/scratch/or/workspace/directory/fireworks/launchpad
LOGDIR_LOC:           /path/to/your/scratch/or/workspace/directory/fireworks/log

FIREWORKS_DB:         fireworks
FIREWORKS_USER:       fireworks
FIREWORKS_PWD:        fireworks

SSL: true
SSL_CA_CERTS: /path/to/root/ca/certificate
SSL_CERTFILE: /path/to/ssl/certificate
SSL_KEYFILE:  /path/to/ssl/key
```

Make sure to adapt the configuration to your needs. Next, understand what
`fwrlm` can do with `fwrlm --help`, i.e.:

```console
fwrlm --help
usage: fwrlm [-h] [--debug] [--verbose] [--log [LOG]]
             {start,status,stop,restart,test,config} ...

Manages FireWorks rocket launchers and associated scripts as daemons.

positional arguments:
  {start,status,stop,restart,test,config}
                        command
    start               Start daemons.
    status              Query daemon status.
    stop                Stop daemons.
    restart             Restart daemons.
    test                Runs service directly without detaching.
    config              Operate on FireWorks config.

optional arguments:
  -h, --help            show this help message and exit
  --debug               debug (default: False)
  --verbose             verbose (default: False)
  --log [LOG]           Write log fwrlm.log, optionally specify log name
                        (default: None)
```

You can get usage information for sub commands as well, i.e.

```console
$ fwrlm config --help
usage: fwrlm config [-h] [--config-dir CONFIG_DIR] [--skel-dir SKEL_DIR]
                    [--template-dir TEMPLATE_DIR]
                    {reset,show} ...

positional arguments:
  {reset,show}          sub-command
    reset               Reset FireWorks config in 'CONFIG_DIR' by first
                        copying files from 'SKEL_DIR' and then rendering files
                        from 'TEMPLATE_DIR' with parameters defined within
                        your 'FWRLM_config.yaml'.
    show                Displays current config.

optional arguments:
  -h, --help            show this help message and exit
  --config-dir CONFIG_DIR
                        User config directory
  --skel-dir SKEL_DIR   Config skeleton directory
  --template-dir TEMPLATE_DIR
                        Config template directory
```

In the following, display detailed information on what is happening by adding
the `--verbose` or `--debug` flags right behind `fwrlm` *before* any
sub-command, i.e. `fwrlm --verbose config reset`. Many `fwrlm` commands will
not display any output otherwise.

Check whether the `FWRLM_config.yaml` is parsed correclty with
`fwrlm config show`. You should see a list of parameters, including those set
within `FWRLM_config.yaml`. Other parameters are filled with default values.
In case of curiosity, look at `imteksimfw/fireworks/fwrlm_config.py` for
further explanations on parameters in the code comments.

Now, running `fwrlm config reset` will do two things:
- Copy all files from `FW_CONFIG_SKEL_PREFIX` specified within
  `FWRLM_config.yaml` to `FW_CONFIG_PREFIX`. Former points to this package's
  `imteksimfw/fireworks/examples/fw_config` per default. Latter should always
  point to `.fireworks` below your home directory.
- Fill all Jinja2 templates within `FW_CONFIG_TEMPLATE_PREFIX` with parameters
  specified in `FWRLM_config.yaml` and place them in `FW_CONFIG_PREFIX`.
  If file names in `FW_CONFIG_SKEL_PREFIX` and `FW_CONFIG_TEMPLATE_PREFIX`
  conflict, then latter overrides former. All key - value pairs in
  `FWRLM_config.yaml` can be used within the templates.

Modify skeleton and templates as see fit. If `FW_CONFIG_PREFIX` exists
already, use `fwrlm config reset --force` to remove it completely before
regenerating the configuration file set.

Afterwards, inspect the files within your `FW_CONFIG_PREFIX` directory.

**Notes**:
- If you want to directly use config skeleton and templates from this package
  unmodified, you can just remove `FW_CONFIG_SKEL_PREFIX` and
  `FW_CONFIG_TEMPLATE_PREFIX` from your `FWRLM_config.yaml`. Otherwise, cloning
  this repository, applying custom modifications to the templates, and
  pointing `FW_CONFIG_SKEL_PREFIX` and `FW_CONFIG_TEMPLATE_PREFIX` to your
  local repository instead is a good practice.
- The parameters `MACHINE` and `SCHEDULER` determine which worker
  and queue adapter files will be used for `rlaunch` and `qlaunch`.
  `MACHINE: JUWELS` and `SCHEDULER: SLURM` will result in `rlaunch` to run with
  the `juwels_noqueue_worker.yaml` worker file, and `qlaunch` to run with the
  `juwels_queue_worker.yaml` worker, the `forhlr2_slurm_qadapter.yaml` queue
  adapter and the `forhlr2_slurm_submit_script.template` submit script template
  files. You can override this default behavior (and make the `MACHINE` and
  `SCHEDULER` key words obsolete) by explicitly setting the
  `RLAUNCH_FWORKER_FILE`, `QLAUNCH_FWORKER_FILE` and `QADAPTER_FILE` parameters
  within `FWRLM_config.yaml`). Note, however, that the qlaunch worker file and
  submit script template must be sepcified within the `QADAPTER_FILE`
  explicitly.
- Default queue adapter file templates expect a bash script
  `${HOME}/.fireworks_env` that makes your FireWorks environment available, i.e.
  by loading environment modules or activating the right virtual environment.
  Make sure to make this file available or modify the particular option within
  your queue adapter file template.

### Launching FWRLM services

After generating a valid configuration, try to establish an ssh connection
to your data base server with `fwrlm --verbose start ssh`. Check whether the
ssh service keeps running with
```console
$ fwrlm --verbose status ssh
INFO: ssh running.
```
`fwrlm status` returns exit code `0` if the service is running and exit code
`> 0` otherwise, allowing for automized polling of a service's state.
Check with
```console
$ fwrlm status ssh
$ echo $?
0
```

The `--verbose` and `--debug` flags do not only specify the verbosity of the
launcher, but also of the evokes background service as well. If `fwrlm status`
indicates a service not to be running after the `start` command, then
check the log files within the directory `LOGDIR_LOC` specified in
your `FWRLM_config.yaml`. Log files are named by the pattern
`{SERVICE_NAME}_{EVOCATION_TIME}.{err,out}`, i.e.
`ssh_tunnel_20200327151733693305.out` and
`ssh_tunnel_20200327151733693305.err`. If the messages do not give enough
information on a possible error source, then restart the service with a
higher verbosity level, i.e. `fwrlm --debug start ssh` and check again.

Usually, the `ssh` service must run before starting any other FireWorks
services. Next, you can continue to launch services individually or in sets.
Try to start an unknown service to see a list of all available services and
service sets, i.e.

```console
$fwrlm start bla
usage: fwrlm start [-h] DAEMON [DAEMON ...]
fwrlm start: error: argument DAEMON: invalid choice: 'bla' (choose from 'ssh', 'recover', 'dummy', 'local-fw', 'webgui', 'local-worker', 'qlaunch', 'hpc-worker', 'hpc-fw', 'rlaunch', 'all')
available
```

See the definitions of `DEMON_DICT` and `DAEMON_SETS` within
`imteksimfw/fireworks/scripts/fwrlm_run.py` for the meaning these sets.

For example, `fwrlm --verbose start hpc-fw` will launch `rlaunch`, `qlaunch`
and `lpad recover_offline` services, comprising the core services necessary
on a typical HPC platform:

```console
$fwrlm --verbose start hpc-fw
INFO: recover started.
INFO: qlaunch started.
INFO: rlaunch started.

$ fwrlm --verbose status hpc-fw
INFO: rlaunch running.
INFO: qlaunch not running.
INFO: recover running.
```

Here, someting went wrong with the `qlaunch` service. Stop services in a
similar manner:

```console
$ fwrlm --verbose stop hpc-fw
INFO: rlaunch stopped.
INFO: recover stopped.
INFO: qlaunch not running.
```

## Utilities

### render

The helper tool `render` offers a simple command line interface to the
Jinja2 template engine. Refer to `render --help`. Running `render inspect`
without any further options will display an overview on which parameters from
`FWRLM_config.yaml` will be filled into which template files from
`FW_CONFIG_TEMPLATE_PREFIX`, i.e.:

```console
$ render inspect
╒══════════════════════════════╤═══════════════╤═════════════╤════════════╤═════╤═════════════╤══════════════╤═════════╤════════════════╤══════════════╤══════════════╤══════════════╤══════════════╤══════════════════╤═══════════════════╤════════════════════╕
│                              │ FIREWORKS_PWD │ WEBGUI_PORT │ LOGDIR_LOC │ SSL │ SSL_KEYFILE │ MONGODB_HOST │ MACHINE │ FIREWORKS_USER │ MONGODB_PORT │ SSL_CA_CERTS │ FIREWORKS_DB │ SSL_CERTFILE │ FW_CONFIG_PREFIX │ FW_AUTH_FILE_NAME │ SSL_PEM_PASSPHRASE │
├──────────────────────────────┼───────────────┼─────────────┼────────────┼─────┼─────────────┼──────────────┼─────────┼────────────────┼──────────────┼──────────────┼──────────────┼──────────────┼──────────────────┼───────────────────┼────────────────────┤
│ FW_config.yaml               │               │ x           │            │     │             │              │ x       │                │              │              │              │              │ x                │ x                 │                    │
├──────────────────────────────┼───────────────┼─────────────┼────────────┼─────┼─────────────┼──────────────┼─────────┼────────────────┼──────────────┼──────────────┼──────────────┼──────────────┼──────────────────┼───────────────────┼────────────────────┤
│ bwcloud_noqueue_fworker.yaml │               │             │            │     │             │              │         │                │              │              │              │              │                  │                   │                    │
├──────────────────────────────┼───────────────┼─────────────┼────────────┼─────┼─────────────┼──────────────┼─────────┼────────────────┼──────────────┼──────────────┼──────────────┼──────────────┼──────────────────┼───────────────────┼────────────────────┤
│ fireworks_mongodb_auth.yaml  │ x             │             │ x          │ x   │ x           │ x            │         │ x              │ x            │ x            │ x            │ x            │                  │                   │ x                  │
├──────────────────────────────┼───────────────┼─────────────┼────────────┼─────┼─────────────┼──────────────┼─────────┼────────────────┼──────────────┼──────────────┼──────────────┼──────────────┼──────────────────┼───────────────────┼────────────────────┤
│ forhlr2_slurm_qadapter.yaml  │               │             │            │     │             │              │         │                │              │              │              │              │ x                │ x                 │                    │
├──────────────────────────────┼───────────────┼─────────────┼────────────┼─────┼─────────────┼──────────────┼─────────┼────────────────┼──────────────┼──────────────┼──────────────┼──────────────┼──────────────────┼───────────────────┼────────────────────┤
│ juwels_slurm_qadapter.yaml   │               │             │            │     │             │              │         │                │              │              │              │              │ x                │ x                 │                    │
├──────────────────────────────┼───────────────┼─────────────┼────────────┼─────┼─────────────┼──────────────┼─────────┼────────────────┼──────────────┼──────────────┼──────────────┼──────────────┼──────────────────┼───────────────────┼────────────────────┤
│ nemo_moab_qadapter.yaml      │               │             │            │     │             │              │         │                │              │              │              │              │ x                │ x                 │                    │
╘══════════════════════════════╧═══════════════╧═════════════╧════════════╧═════╧═════════════╧══════════════╧═════════╧════════════════╧══════════════╧══════════════╧══════════════╧══════════════╧══════════════════╧═══════════════════╧════════════════════╛
```
