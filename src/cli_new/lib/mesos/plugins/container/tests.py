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
Container plugin tests.
"""

import os
import tempfile
import shutil

import config

import mesos.http as http

from mesos.tests import CLITestCase
from mesos.tests import Master
from mesos.tests import Agent
from mesos.tests import Task
from mesos.tests import capture_output
from mesos.util import Table

from mesos.plugins.container.main import Container as ContainerPlugin


class TestContainerPlugin(CLITestCase):
    """
    Test class for the container plugin.
    """
    def test_list(self):
        """
        Basic test for the container `list()` sub-command.
        """
        # Launch a master, agent, and task.
        master = Master()
        master.launch()

        agent = Agent()
        agent.launch()

        task = Task({"command": "sleep 1000"})
        task.launch()

        # Open the agent's `/containers` endpoint and read the
        # container information ourselves.
        containers = http.get_json(agent.addr, 'containers')

        self.assertEqual(type(containers), list)
        self.assertEqual(len(containers), 1)

        # Invoke the container plugins `list()` command
        # and parse its output as a table.
        plugin = ContainerPlugin(config)
        output = capture_output(plugin.list, {"--agent": agent.addr})
        table = Table.parse(output)

        # Verify there are two rows in the table
        # and that they are formatted as expected,
        # with the proper container info in them.
        self.assertEqual(table.dimensions()[0], 2)
        self.assertEqual(table.dimensions()[1], 3)

        self.assertEqual("Container ID", table[0][0])
        self.assertEqual("Framework ID", table[0][1])
        self.assertEqual("Name", table[0][2])
        self.assertEqual(containers[0]["container_id"], table[1][0])
        self.assertEqual(containers[0]["framework_id"], table[1][1])
        self.assertEqual(containers[0]["executor_id"], table[1][2])

        # Kill the task, agent, and master.
        task.kill()
        agent.kill()
        master.kill()

    @CLITestCase.verify_root()
    @CLITestCase.verify_linux()
    def test_exec(self):
        """
        Basic test for the container 'exec'()' sub-command.
        """
        # Launch a master and agent.
        master = Master()
        master.launch()

        agent_flags = {"isolation": "filesystem/linux,namespaces/pid"}
        agent = Agent(agent_flags)
        agent.launch()

        # Launch a task that writes to a temporary file (for
        # synchronization), and then sleeps for a long time.
        #
        # TODO(klueska): The only reason we need to synchronize on a
        # file is that there is currently no way to know if the 'pid'
        # associated with a container has already been properly
        # isolated yet or not. If it hasn't, we will enter the wrong
        # set of namespaces and cgroups associated with the container.
        # This is a possible security hole, and needs to be fixed.
        tmpdir = tempfile.mkdtemp()
        tmpfile = os.path.join(tmpdir, "tmpfile")

        task_flags = {"command": "touch {file}; sleep 1000"
                                 .format(file=tmpfile),
                      "shell": "true"}
        task = Task(task_flags)
        task.launch()

        # Wait for the task to start running and the file to be written.
        #
        # pylint: disable=missing-docstring,unused-argument
        def wait_for_file(data):
            return os.path.exists(tmpfile)

        containers = http.get_json(agent.addr, 'containers', wait_for_file)

        self.assertEqual(containers[0]["executor_id"], task.flags["name"])

        # Delete the temporary file.
        shutil.rmtree(tmpdir)

        # Invoke `mesos container exec` with the `ps -ax` command.
        # Given the task we launched above, we expect to see exactly 6
        # lines of output from the command (5 running processes plus
        # the `ps` header).
        #
        # 1) The init process of the container (i.e. `mesos-containerizer`).
        # 2) The executor of the container (i.e. `mesos-executor`).
        # 3) `sh -c touch {tmpfile}; sleep 1000`
        # 4) `sleep 1000`
        # 5) `ps -ax`
        plugin = ContainerPlugin(config)

        argv = {"--agent" : agent.addr,
                "<container-id>" : containers[0]["container_id"],
                "<command>" : ["ps", "-ax"]}

        output = capture_output(plugin.execute, argv, {"redirect_io" : True})

        self.assertEqual(len(output.split('\n')), 6)
