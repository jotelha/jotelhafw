#!/usr/bin/env python
#
# render.py
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
"""Quickly renders jinja2 template files."""

import logging
import os
import time
import yaml

from tabulate import tabulate

from jinja2 import Template, Environment, FileSystemLoader
from jinja2 import meta, contextfunction  # , select_autoescape

from ansible.plugins.filter.core import \
    to_yaml, to_nice_yaml, to_json, to_nice_json


def load_context(context):
    """Converts yaml string to dict if necessary.

    Args:
        context (str or dict): jinja2 context, YAML format if str.

    Returns:
        context (dict): jinja2 context
    """
    logger = logging.getLogger(__name__)
    if isinstance(context, str):
        context_str = context
        context = yaml.safe_load(context_str)
        logger.debug("Parsed '{}' as YAML '{}'".format(context_str, context))
    return context


def render_single(
        infile,
        outfile,
        context={'machine': 'NEMO', 'mode': 'PRODUCTION'}):
    """Render single jinja2 template file.

    Example:

    Args:
        infile (str): .yaml emplate file.
        outfile (str):  rendered .yaml file.
        context (str or dict): jinja2 context, YAML format if str.

    Returns:
        Nothing.
    """
    logger = logging.getLogger(__name__)

    context = load_context(context)

    with open(infile) as template_file:
        template = Template(template_file.read())

    rendered = template.render(context)
    logger.debug("Rendered infile as \n{}".format(rendered))

    with open(outfile, "w") as rendered_file:
        rendered_file.write(rendered)

    return

def render_batch(
        template_dir,
        build_dir,
        context={'machine': 'NEMO', 'mode': 'PRODUCTION'}):
    """Render batch of template files within `template_dir`.

    Args:
        template_dir (str): all files within are treated as templates
        build_dir (str): render templates within under their original name
        context (str or dict): jinja2 context, YAML format if str
            Same context used for all templates.
    """
    context = load_context(context)
    env = TailoredEnvironment(template_dir, build_dir, context)
    env.render_all()

# custom jinja2 filters
def datetime(value, format='%Y-%m-%d-%H:%M'):
    if value == 'now':
        return time.strftime(format)
    else:
        return value.strftime(format)


@contextfunction
def get_context(c):
    return c

# output helper functions
def variable_overview(variables):
    """Generates a tabular string representation of variables."""
    lines = [['', *variables['all']]]
    for t, tv in variables.items():
        if t != 'all':
            line = [t]
            line.extend(['x' if v in tv else '' for v in variables['all']])
            lines.append(line)
    return tabulate(lines, tablefmt='fancy_grid')


def inspect(template_dir):
    """Generates a tabular string representation of all undefined variables
    in templates.

    Args:
        template_dir (str): all files within are treated as templates
        build_dir (str): render templates within under their original name
        context (str or dict): jinja2 context, YAML format if str
            Same context used for all templates.
    """
    env = TailoredEnvironment(template_dir)
    undefined = env.find_undefined_variables()
    return variable_overview(undefined)


class TailoredEnvironment(Environment):
    """Jinja2 environment tailored towards use within this package."""
    def __init__(self, template_dir, build_dir=os.getcwd(), context=None):
        self.logger = logging.getLogger(__name__)
        super().__init__(
          loader=FileSystemLoader(template_dir),
          autoescape=False,
          extensions=['jinja2_time.TimeExtension'])
        #  autoescape=select_autoescape(['yaml']))
        # register filters and functions:
        self.filters['datetime'] = datetime
        self.filters['to_yaml'] = to_yaml
        self.filters['to_nice_yaml'] = to_nice_yaml
        self.filters['to_json'] = to_json
        self.filters['to_nice_json'] = to_nice_json
        self.globals['context'] = get_context
        self.globals['callable'] = callable

        self.build_dir = build_dir
        self.context = None

    def render_all(self):
        """Render all templates with same context."""
        for template_name in self.list_templates():
            self.logger.info("Process template {:s}.".format(template_name))
            self.render_template(template_name)

    def render_template(self, template_name, outfile_name=None, context=None):
        """Render single template."""
        template = self.get_template(template_name)
        if not outfile_name:
            outfile_name = template_name

        if not context:
            context = self.context

        output = template.render(context)
        outfile_name = os.path.join(self.build_dir, outfile_name)
        self.logger.info("Render template '{:s}' to '{:s}'."
            .format(template_name, outfile_name))
        self.logger.debug("    with context {}.".format(context))
        with open(outfile_name, 'w') as of:
            of.write(output)

    def find_undefined_variables(self):
        """Finds all variables within template source"""
        template_variables = {'all': set()}
        for template_name in self.list_templates():
            self.logger.info("Loading template {:s}.".format(template_name))
            template_source = self.loader.get_source(self, template_name)[0]
            try:
                parsed_content = self.parse(template_source)
            except Exception as exc:
                self.logger.exception(
                    "Failed parsing template '{:s}' with exception '{}'."
                        .format(template_name, exc))
                raise exc
            template_variables[template_name] = meta.find_undeclared_variables(
                parsed_content)
            template_variables['all'] = \
                template_variables['all'] | template_variables[template_name]
        return template_variables
