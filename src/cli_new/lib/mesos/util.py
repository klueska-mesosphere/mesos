# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
A collection of helper functions used by the CLI and its Plugins.
"""

import imp
import importlib
import json
import os
import textwrap
import urllib2

from mesos.exceptions import CLIException


def import_modules(package_paths, module_type):
    """
    Looks for python packages under `package_paths` and imports
    them as modules. Returns a dictionary of the basename of the
    `package_paths` to the imported modules.
    """
    modules = {}
    for package_path in package_paths:
        # We put the imported module into the namespace of
        # "mesos.<module_type>.<>" to keep it from cluttering up
        # the import namespace elsewhere.
        package_name = os.path.basename(package_path)
        package_dir = os.path.dirname(package_path)
        module_name = "mesos." + module_type + "." + package_name
        try:
            module = importlib.import_module(module_name)
        except Exception:
            obj, filename, data = imp.find_module(package_name, [package_dir])
            module = imp.load_module(module_name, obj, filename, data)
        modules[package_name] = module

    return modules


def get_module(modules, import_path):
    """
    Given a modules dictionary returned by `import_modules()`,
    return a reference to the module at `import_path` relative
    to the base module. For example, get_module(modules, "example.stuff")
    will return a reference to the "stuff" module inside the
    imported "example" plugin.
    """
    import_path = import_path.split('.')
    try:
        module = modules[import_path[0]]
        if len(import_path) > 1:
            module = getattr(module, ".".join(import_path[1:]))
    except Exception as exception:
        raise CLIException("Unable to get module: {error}"
                           .format(error=str(exception)))

    return module


def completions(comp_words, current_word, argv):
    """
    Helps autocomplete by returning the appropriate
    completion words under three conditions.

    1) Returns `comp_words` if the completion word is
       potentially in that list.
    2) Returns an empty list if there is no possible
       completion.
    3) Returns `None` if the autocomplete is already done.
    """
    comp_words += ["-h", "--help", "--version"]

    if len(argv) == 0:
        return comp_words

    if len(argv) == 1:
        if argv[0] not in comp_words and current_word:
            return comp_words

        if argv[0] in comp_words and current_word:
            return comp_words

        if argv[0] not in comp_words and not current_word:
            return []

        if argv[0] in comp_words and not current_word:
            return None

    if len(argv) > 1 and argv[0] not in comp_words:
        return []

    if len(argv) > 1 and argv[0] in comp_words:
        return None

    raise CLIException("Unreachable")


def format_commands_help(cmds):
    """
    Helps format plugin commands for display.
    """
    longest_cmd_name = max(cmds.keys(), key=len)

    help_string = ""
    for cmd in sorted(cmds.keys()):
        # For the top-level entry point, `cmds` is a single-level
        # dictionary with `short_help` as the values. For plugins,
        # `cmds` is a two-level dictionary, where `short_help` is a
        # field in each sub-dictionary.
        short_help = cmds[cmd]
        if isinstance(short_help, dict):
            short_help = short_help["short_help"]

        num_spaces = len(longest_cmd_name) - len(cmd) + 2
        help_string += "  %s%s%s\n" % (cmd, " " * num_spaces, short_help)

    return help_string


def format_subcommands_help(cmd):
    """
    Helps format plugin subcommands for display.
    """
    arguments = " ".join(cmd["arguments"])
    short_help = cmd["short_help"]
    long_help = textwrap.dedent(cmd["long_help"].rstrip())
    long_help = "  " + "\n  ".join(long_help.split('\n'))
    flags = cmd["flags"]
    flags["-h --help"] = "Show this screen."
    flag_string = ""

    if len(flags.keys()) != 0:
        longest_flag_name = max(flags.keys(), key=len)
        for flag in sorted(flags.keys()):
            num_spaces = len(longest_flag_name) - len(flag) + 2
            flag_string += "  %s%s%s\n" % (flag, " " * num_spaces, flags[flag])

    return (arguments, short_help, long_help, flag_string)

def hit_endpoint(addr, endpoint):
    """
    Hit the specified endpoint and return the results as JSON.
    """
    try:
        url = "http://{addr}/{endpoint}".format(addr=addr, endpoint=endpoint)

        http_response = urllib2.urlopen(url).read().decode("utf-8")
    except Exception as exception:
        raise CLIException("Could not open '{url}': {error}"
                           .format(url=url, error=str(exception)))

    try:
        return json.loads(http_response)
    except Exception as exception:
        raise CLIException("Could load JSON from '{url}': {error}"
                           .format(url=url, error=str(exception)))

def read_file(addr, path):
    """
    Read a file from a master / agent node's sandbox.
    """
    # It is undocumented, but calling the `/files/read` endpoint
    # and setting `offset=-1` returns the length of the file. We
    # leverage this here to first get the length of the file
    # before reading it. Unfortunately, there is no way to just
    # read the file without first getting its length.
    try:
        endpoint = "files/read?path={path}&offset=-1".format(path=path)
        data = hit_endpoint(addr, endpoint)
    except Exception as exception:
        raise CLIException("Could not read file length '{path}': {error}"
                           .format(path=path, error=str(exception)))

    length = data['offset']
    if length == 0:
        return

    # Read the file in 1024 byte chunks and yield the results to
    # the caller. Reading this way allows us to get real-time
    # updates of the data instead of waiting for the whole file to
    # be downloaded.
    offset = 0
    chunk_size = 1024
    if (length - offset) < chunk_size:
        chunk_size = length - offset

    while True:
        try:
            endpoint = ("files/read?"
                        "path={path}&"
                        "offset={offset}"
                        "&length={length}"
                        .format(path=path,
                                offset=offset,
                                length=chunk_size))

            data = hit_endpoint(addr, endpoint)
        except Exception as exception:
            raise CLIException("Could not read from file '{path}'"
                               " at offset '{offset}: {error}"
                               .format(path=path,
                                       offset=offset,
                                       error=str(exception)))

        yield data['data']

        offset += len(data['data'])
        if offset == length:
            break

        if (length - offset) < chunk_size:
            chunk_size = length - offset

# pylint: disable=R0903
class Table(object):
    """ Defines a custom table structure for printing to the terminal. """

    # Takes a list of column names
    def __init__(self, columns):
        self.padding = []
        self.table = [columns]
        for column in columns:
            self.padding.append(len(column))

    # Takes a row entry for every column
    def add_row(self, row):
        """
        Adds a row to the table. Input must be a list where each entry
        corresponds to its respective column in order.
        """
        # Number of entries and columns do not match
        if len(row) != len(self.table[0]):
            raise CLIException("Number of entries and columns do no match!")

        # Adjust padding for each column
        for index, elem in enumerate(row):
            if len(elem) > self.padding[index]:
                self.padding[index] = len(elem)

        self.table.append(row)

    def __str__(self):
        """
        Converts table to string for printing.
        """
        table_string = ""
        for r_index, row in enumerate(self.table):
            for index, entry in enumerate(row):
                table_string += "%s%s" % \
                        (entry, " " * (self.padding[index] - len(entry) + 2))

            if r_index != len(self.table) - 1:
                table_string += "\n"

        return table_string
