#!/usr/bin/env python
#
# tracer.py
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
"""Offers decorator for tracing with hunter module if available."""

import functools
import sys
try:  # allow for simple tracing when hunter tracer is available
    import hunter
    TRACE = True
except Exception:
    TRACE = False


# inspired by https://python-hunter.readthedocs.io/en/latest/cookbook.html#probe-lightweight-tracing
def trace_func(call_printer_stream=sys.stderr, vars_snooper_stream=sys.stderr, module=__name__):
    """Trace function call to custom stream."""
    def decorator_trace_func(func):
        @functools.wraps(func)
        def wrapper_trace_func(*args, **kwargs):
            # try to set up tracer, fails if 'hunter' module not available
            if TRACE:
                with hunter.trace(
                        module=module,
                        stdlib=False,
                        threading_support=False,
                        actions=[
                            hunter.CallPrinter(stream=call_printer_stream),
                            hunter.VarsSnooper(stream=vars_snooper_stream),
                        ]
                    ):
                    return func(*args, **kwargs)
            else:
                return func(*args, **kwargs)
        return wrapper_trace_func
    return decorator_trace_func
