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
Testing for the Agent Plugin
"""

import unittest

import mesos.plugins.agent.main
from mesos.util import hit_endpoint
from mesos.exceptions import CLIException

from mesos.PluginTestBase import PluginTestBase

# pylint: disable=C0103
class Test_AgentPlugin(PluginTestBase):
    """
    Test class for the container plugin. Inherits from PluginTestBase.
    A cluster will be spawned by the setup methods in the base class which
    will be used to check commands against.
    """
    config = None

    @classmethod
    def setUpClass(cls):
        """
        We override the default setUpClass found in PluginTestBase.
        This is because our test requires a different setup, i.e. it does not
        require a task being executed on the cluster.
        """
        try:
            cls.launch_master()
        except Exception as exception:
            cls.error_log += ("Failed to set up master node: {error}"
                              .format(error=exception))

        try:
            cls.launch_agent()
        except Exception as exception:
            cls.error_log += ("Failed to set up agent node: {error}"
                              .format(error=exception))

        try:
            cls.check_stable()
        except Exception as exception:
            cls.error_log += ("Failed to stabilize cluster: {error}"
                              .format(error=exception))

    @classmethod
    def tearDownClass(cls):
        """
        We override the default tearDownClass found in PluginTestBase.
        This is because our test requires a different teardown,
        i.e. we dont have a mesos-execute instance to bring down.
        """
        try:
            cls.kill_master()
        except Exception as exception:
            raise CLIException("Failed to tear down master node: {error}"
                               .format(error=exception))

        try:
            cls.kill_agent()
        except Exception as exception:
            raise CLIException("Failed to tear down agent node: {error}"
                               .format(error=exception))

    def test_ping(self):
        """
        Tests agent ping.
        We check if the command prints the appropiate message.
        """
        if not self.setup_success:
            raise unittest.SkipTest("Error setting up cluster in test setup:"
                                    " {log}".format(log=self.error_log))

        agent_plugin = mesos.plugins.agent.main.Agent(self.config)
        ping_argv = {"--addr" : "127.0.0.1:5051"}

        try:
            output = PluginTestBase.get_output(agent_plugin.ping, ping_argv)
        except Exception as exception:
            raise CLIException("Could not get command output: {error}"
                               .format(error=exception))

        self.assertEqual("Agent Healthy!", output)

    def test_state(self):
        """
        Tests agent state.
        We check for different state information from the agent and check
        against the actual state json from the agent /state endpoint.
        """
        if not self.setup_success:
            raise unittest.SkipTest("Error setting up cluster in test setup:"
                                    " {log}".format(log=self.error_log))

        # We get the entire agent state info to check out parsing againt
        try:
            agent_state = hit_endpoint('127.0.0.1:5051', '/state')
        except Exception as exception:
            self.fail("Could not get state from agent node: {error}"
                      .format(error=exception))

        agent_plugin = mesos.plugins.agent.main.Agent(self.config)
        # Now we proced to check if the fields parsed are correct
        agent_argv = {"--addr" : "127.0.0.1:5051",
                      "<field>" : ["master_hostname"]}

        try:
            test_response = PluginTestBase.get_output(agent_plugin.state,
                                                      agent_argv)
        except Exception as exception:
            self.fail("Could not get master_hostname from agent node: {error}"
                      .format(error=exception))

        self.assertEqual(test_response[1:-1], agent_state["master_hostname"])
        agent_argv["<field>"] = ["flags.port"]

        try:
            test_response = PluginTestBase.get_output(agent_plugin.state,
                                                      agent_argv)
        except Exception as exception:
            self.fail("Could not get flags.port from agent node: {error}"
                      .format(error=exception))

        self.assertEqual(test_response[1:-1], agent_state["flags"]["port"])
