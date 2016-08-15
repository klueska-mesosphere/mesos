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
The agent plugin
"""
import json
import urllib2

import mesos.util

from mesos.exceptions import CLIException
from mesos.plugins import PluginBase

PLUGIN_CLASS = "Agent"
PLUGIN_NAME = "agent"

VERSION = "Mesos CLI Agent Plugin 1.0"

SHORT_HELP = "Agent specific commands for the Mesos CLI"


class Agent(PluginBase):
    """
    Agent plugin class. Inherits from Plugins Base.
    """

    COMMANDS = {
        "ping" : {
            "arguments" : [],
            "flags" : {
                "--addr=<addr>" : "IP and port of agent " + \
                                  "[default: {agent_ip}:{agent_port}]"
            },
            "short_help" : "Check Health of a running agent",
            "long_help"  :
                """
                Sends a HTTP healthcheck request and returns the result.
                """
        },
        "state" : {
            "arguments" : ["[<field>...]"],
            "flags" : {
                "--addr=<addr>" : "IP and port of agent " + \
                                  "[default: {agent_ip}:{agent_port}]"
            },
            "short_help" : "Get Agent State Information",
            "long_help"  :
                """
                Get the agent state from the /state endpoint. If no <field> is
                supplied, it will display the entire state json. A field may be
                parsed from the state json to get its value. The format is
                field or index seperated by a \'.\'
                So for example, getting the work_dir from flags is:
                flags.work_dir
                Getting checkpoint information for the first framework would be:
                frameworks.0.checkpoint

                Multiple fields can be parsed from the same command.
                I.e. flags.work_dir id
                """
        }
    }

    # pylint: disable=R0201
    def __is_int(self, num):
        """
        Checks to see if a given value is an integer or not.
        """
        try:
            int(num)
            return True
        except ValueError:
            return False

    def __parse_json(self, fields, json_info):
        """
        This method parses a given json according to the field
        provided. The fields variable is a list of strings that
        specify which entry to parse. The format is field or index
        seperated by a \'.\'
        So for example, getting the work_dir from flags is:
        flags.work_dir

        Getting checkpoint information for the first framework would be:
        frameworks.0.checkpoint
        Here 0 signifies the first entry of the list for key 'framework'
        """
        split_fields = fields.split(".")
        json_sub = json_info

        for field in split_fields:
            if isinstance(json_sub, dict):
                if self.__is_int(field):
                    raise CLIException("JSON is not a list, "
                                       "not expecting index!")
                if field in json_sub:
                    json_sub = json_sub[field]
                else:
                    raise CLIException("JSON field : {field} not found!"
                                       .format(field=field))
            elif isinstance(json_sub, list):
                if not self.__is_int(field):
                    raise CLIException("JSON is a list,"
                                       " not expecting non-integer!")
                index = int(field)
                if index >= len(json_sub):
                    raise CLIException("List index out of bound!")
                json_sub = json_sub[index]
            else:
                raise CLIException("No further sub-fields for : {field}"
                                   .format(field=field))

        return json_sub

    def ping(self, argv):
        """
        Checks the HTTP response from an agent /health endpoint.
        """
        http_response_code = None
        try:
            http_response_code = urllib2.urlopen("http://{addr}/health"
                                                 .format(addr=argv["--addr"])) \
                                                .getcode()
        except Exception as exception:
            raise CLIException(("Could not establish connection with agent: "
                                "{error}".format(error=exception)))

        if http_response_code == 200:
            print "Agent Healthy!"
        else:
            print "Agent not healthy!"

    def state(self, argv):
        """
        Parses information from the agent /state endpoint in a convenient
        way. Relies on __parse_json to do the actual parsing.
        """
        try:
            state = mesos.util.hit_endpoint(argv["--addr"], "/state")
        except Exception as exception:
            raise CLIException("Unable to read from '/state' endpoint: {error}"
                               .format(error=exception))

        if len(argv["<field>"]) == 0:
            print json.dumps(state, indent=2)
            return

        for arg in argv["<field>"]:
            try:
                result = self.__parse_json(arg, state)
            except Exception as exception:
                print ("Error parsing json for {field} : {error}"
                       .format(field=arg,
                               error=exception))
                continue
            print json.dumps(result, indent=2)
