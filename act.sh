#!/bin/bash

# https://github.com/nektos/act/issues/418#issuecomment-727261137
docker build -t ubuntu-builder etc/ubuntu-builder
act -P ubuntu-latest=ubuntu-builder