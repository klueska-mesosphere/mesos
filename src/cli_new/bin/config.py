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
This file defines the default configuration of the mesos-cli. It also takes
care of updating the default configuration from reading environment variables
or parsing a configuration file.
"""

import json
import os
import subprocess
import sys

from mesos.exceptions import CLIException


# There is no version module included in this package. However,
# when creating an executable using pyinstaller, a version.py
# file will be autogenerated and inserted into the PYTHONPATH.
# When this happens we import it to set the VERSION.
try:
    # pylint: disable=F0401,W0611
    from version import VERSION
except Exception:
    VERSION = "Development"

# The top-level directory of this project.
PROJECT_DIR = os.path.join(os.path.dirname(__file__), os.pardir)

# Default IP for the agent.
AGENT_IP = "127.0.0.1"
AGENT_PORT = "5051"

# Default IP for the master.
MASTER_IP = "127.0.0.1"
MASTER_PORT = "5050"

# Default set of SSH_KEYS to use for remote agents
SSH_KEYS = {}

# The builtin plugins.
PLUGINS = [
    os.path.join(PROJECT_DIR, "lib/mesos/plugins", "agent"),
    os.path.join(PROJECT_DIR, "lib/mesos/plugins", "container")
]

# Absolute directory to the executables required by commands and test cases
# If left empty string, assumed they are in the path. Path must end with a /
# Example: '/tmp/bin/'
EXECUTABLE_DIR = ''

# Allow extra plugins to be pulled in from a configuration file.
if os.environ.get("MESOS_CLI_CONFIG_FILE"):
    try:
        CONFIG_FILE = open(os.environ["MESOS_CLI_CONFIG_FILE"])
    except Exception as exception:
        sys.exit("Unable to open configuration file '{config}': {error}"
                 .format(config=os.environ.get("MESOS_CLI_CONFIG_FILE"),
                         error=str(exception)))

    try:
        CONFIG_DATA = json.load(CONFIG_FILE)
    except Exception as exception:
        raise CLIException("Error loading config file as JSON: {error}"
                           .format(error=exception))

    if "agent_ip" in CONFIG_DATA:
        if not isinstance(CONFIG_DATA["agent_ip"], basestring):
            raise CLIException("'agent_ip' field must be a string")

        AGENT_IP = CONFIG_DATA["agent_ip"]

    if "agent_port" in CONFIG_DATA:
        if not isinstance(CONFIG_DATA["agent_port"], basestring):
            raise CLIException("'agent_port' field must be a string")

        AGENT_PORT = CONFIG_DATA["agent_port"]

    if "executable_dir" in CONFIG_DATA:
        if not isinstance(CONFIG_DATA["executable_dir"], basestring):
            raise CLIException("'master_port'  field must be a string")
        if len(CONFIG_DATA["executable_dir"]) != 0 \
                and not CONFIG_DATA["executable_dir"].endswith('/'):
            raise CLIException("exectuable_dir must end with /")

        EXECUTABLE_DIR = CONFIG_DATA["executable_dir"]

    if "master_ip" in CONFIG_DATA:
        if not isinstance(CONFIG_DATA["master_ip"], basestring):
            raise CLIException("'master_ip' field must be a string")

        MASTER_IP = CONFIG_DATA["master_ip"]

    if "master_port" in CONFIG_DATA:
        if not isinstance(CONFIG_DATA["master_port"], basestring):
            raise CLIException("'master_port' field must be a string")

        MASTER_PORT = CONFIG_DATA["master_port"]

    if "ssh_keys" in CONFIG_DATA:
        if not isinstance(CONFIG_DATA["ssh_keys"], dict):
            raise CLIException("'ssh_keys' field must be a dict")

        SSH_KEYS = dict(SSH_KEYS.items() + \
                        CONFIG_DATA["ssh_keys"].items())

    if "plugins" in CONFIG_DATA:
        if not isinstance(CONFIG_DATA["plugins"], list):
            raise CLIException("'plugins' field must be a list")

        PLUGINS.extend(CONFIG_DATA["plugins"])


# Allow extra plugins to be pulled in from the environment.
# The `MESOS_CLI_PLUGINS` environment variable is a ":" separated
# list of paths to each plugin. All paths must be absolute.
if os.environ.get("MESOS_CLI_PLUGINS"):
    PLUGINS += filter(None, os.environ.get("MESOS_CLI_PLUGINS").split(":"))

# Override the agent IP and port from the environment. We use the
# standard Mesos environment variables for `MESOS_IP`,
# `MESOS_IP_DISCOVERY_COMMAND` and `MESOS_PORT` to get the agent IP
# and port. We also provide our own `MESOS_CLI_AGENT_IP` and
# `MESOS_CLI_AGENT_PORT` environment variables as a way of overriding
# the others.
if os.environ.get("MESOS_IP_DISCOVERY_COMMAND"):
    try:
        AGENT_IP = subprocess.check_output(
            os.environ.get("MESOS_IP_DISCOVERY_COMMAND"),
            shell=True).strip()
    except Exception as exception:
        sys.exit("Unable to run MESOS_IP_DISCOVERY_COMMAND: {error}"
                 .format(error=str(exception)))

if os.environ.get("MESOS_IP"):
    AGENT_IP = os.environ.get("MESOS_IP").strip()

if os.environ.get("MESOS_CLI_AGENT_IP"):
    AGENT_IP = os.environ.get("MESOS_CLI_AGENT_IP").strip()

if os.environ.get("MESOS_PORT"):
    AGENT_PORT = os.environ.get("MESOS_PORT").strip()

if os.environ.get("MESOS_CLI_AGENT_PORT"):
    AGENT_PORT = os.environ.get("MESOS_CLI_AGENT_PORT").strip()

if os.environ.get("MESOS_CLI_MASTER_IP"):
    MASTER_IP = os.environ.get("MESOS_CLI_MASTER_IP").strip()

if os.environ.get("MESOS_CLI_MASTER_PORT"):
    MASTER_PORT = os.environ.get("MESOS_CLI_MASTER_PORT").strip()
