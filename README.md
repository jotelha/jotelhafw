# Custom FireWorks extensions

[![PyPI](https://img.shields.io/pypi/v/jlhfw)](https://pypi.org/project/jlhfw/) [![Tests](https://img.shields.io/github/actions/workflow/status/jotelha/jlhfw/test.yml?branch=main)](https://github.com/jotelha/jlhfw/actions/workflows/test.yml)

Johannes Hörmann, johannes.hoermann@imtek.uni-freiburg.de, Mar 2020


This repository contains custom fireworks tasks and helper scripts used for conducting computational parametric studies presented in 

> J. L. Hörmann, C. (刘宸旭) Liu, Y. (孟永钢) Meng, and L. Pastewka, “Molecular simulations of sliding on SDS surfactant films,” The Journal of Chemical Physics, vol. 158, no. 24, p. 244703, Jun. 2023, doi: [10.1063/5.0153397](https://doi.org/10.1063/5.0153397).

# Quick start

Install the official FireWorks package, i.e. by `pip install fireworks`,
(https://github.com/materialsproject/fireworks) and subsequently make this
package available to your FireWorks environment, i.e. by
`pip install jlhfw`.

## Custom FireTasks quick start

To use custom FireTasks within `jlhfw`, append

    ADD_USER_PACKAGES:
      - jlhfw.fireworks.user_objects.firetasks

to your `~/.fireworks/FW_config.yaml`.

Configuration samples part of the [FireWorks RocketLauncher Manager](https://github.com/jotelha/fwrlm)
include this line already.
