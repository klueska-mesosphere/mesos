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
The container plugin.
"""


import mesos.http as http

from mesos.exceptions import CLIException
from mesos.plugins import PluginBase
from mesos.util import Table


PLUGIN_CLASS = "Container"
PLUGIN_NAME = "container"

VERSION = "Mesos CLI Container Plugin 0.1"

SHORT_HELP = "Container specific commands for the Mesos CLI"


class Container(PluginBase):
    """
    The container plugin.
    """

    COMMANDS = {
        "list" : {
            "arguments" : [],
            "flags" : {
                "--agent=<addr>" : "IP and port of agent " + \
                                   "[default: {agent_ip}:{agent_port}]"
            },
            "short_help" : "List all running containers on an agent",
            "long_help"  : """\
                Lists all running containers on an agent.
                """
        },
    }

    def list(self, argv):
        """
        Lists all containers on an agent.
        """
        try:
            containers = http.get_json(argv["--agent"], "containers")
        except Exception as exception:
            raise CLIException("Could not open '/containers'"
                               " endpoint at '{addr}': {error}"
                               .format(addr=argv["--agent"],
                                       error=exception))

        if len(containers) == 0:
            print ("There are no containers running on agent '{addr}'."
                   .format(addr=argv["--agent"]))
            return

        try:
            table = Table(["Container ID", "Framework ID", "Name"])
            for container in containers:
                table.add_row([container["container_id"],
                               container["framework_id"],
                               container["executor_id"]])
        except Exception as exception:
            raise CLIException("Unable to build table of containers: {error}"
                               .format(error=exception))

        print str(table)
