# IMTEK-Simulation custom FireWorks extensions

[![PyPI](https://img.shields.io/pypi/v/imteksimfw)](https://pypi.org/project/imteksimfw/) [![Tests](https://img.shields.io/github/workflow/status/IMTEK-Simulation/imteksimfw/test?label=tests)](https://github.com/IMTEK-Simulation/imteksimfw/actions?query=workflow%3Atest)


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

Configuration samples part of the [FireWorks RocketLauncher Manager](https://github.com/jotelha/fwrlm)
include this line already.
