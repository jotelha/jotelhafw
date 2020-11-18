# IMTEK-Simulation custom FireWorks extensions

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