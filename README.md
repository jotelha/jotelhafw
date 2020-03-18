# Curstom FireWorks extensions

Johannes Hörmann, johannes.hoermann@imtek.uni-freiburg.de, Mar 2020

To use custom FireTasks, make this package available to your FireWorks
environment, i.e. by `pip install imteksimfw`, and append

    ADD_USER_PACKAGES:
      - imteksimfw.fireworks.user_objects.firetasks

to your `~/.fireworks/FW_config.yaml`.
