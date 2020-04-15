# -*- python -*-
# https://github.com/TACC/Lmod/blob/master/init/env_modules_python.py.in
# modified 15th April 2020
#
#--------------------------------------------------------------------------
#-- Lmod License
#--------------------------------------------------------------------------
#--
#--  Lmod is licensed under the terms of the MIT license reproduced below.
#--  This means that Lmod is free software and can be used for both academic
#--  and commercial purposes at absolutely no cost.
#--
#--  ----------------------------------------------------------------------
#--
#--  Copyright (C) 2008-2018 Robert McLay
#--
#--  Permission is hereby granted, free of charge, to any person obtaining
#--  a copy of this software and associated documentation files (the
#--  "Software"), to deal in the Software without restriction, including
#--  without limitation the rights to use, copy, modify, merge, publish,
#--  distribute, sublicense, and/or sell copies of the Software, and to
#--  permit persons to whom the Software is furnished to do so, subject
#--  to the following conditions:
#--
#--  The above copyright notice and this permission notice shall be
#--  included in all copies or substantial portions of the Software.
#--
#--  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
#--  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
#--  OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
#--  NONINFRINGEMENT.  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
#--  BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
#--  ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
#--  CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#--  THE SOFTWARE.
#--
#--------------------------------------------------------------------------
from __future__ import print_function
from subprocess import PIPE, Popen
import os
import sys
import logging

def module(command, *arguments):
    logger = logging.getLogger(__name__)
    numArgs = len(arguments)
    lmod_cmd = os.path.join(os.environ['MODULESHOME'], 'libexec', 'lmod')
    A = [ lmod_cmd, 'python', command]
    if (numArgs == 1):
        A += arguments[0].split()
    else:
        A += list(arguments)

    logger.debug("Evoking lmod command '{}'...".format(A))
    proc           = Popen(A, stdout=PIPE, stderr=PIPE)
    stdout, stderr = proc.communicate()
    if (os.environ.get('LMOD_REDIRECT','@redirect@') != 'no'):
        err_out=sys.stdout

    logger.info("lmod stderr:")
    logger.info(stderr.decode())
    logger.debug("lmod stdout:")
    logger.debug(stdout.decode())
    exec(stdout.decode())
