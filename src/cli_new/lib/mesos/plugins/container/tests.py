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
Testing for the Container Plugin
"""
import os
import unittest

from multiprocessing import Process, Manager

import mesos.plugins.container.main

from mesos.exceptions import CLIException
from mesos.util import hit_endpoint
from mesos.PluginTestBase import PluginTestBase

# pylint: disable=C0103
class Test_ContainerPlugin(PluginTestBase):
    """
    Test class for the container plugin. Inherits from PluginTestBase.
    A cluster will be spawned by the setup methods in the base class which
    will be used to check commands against.
    """
    config = None

    # pylint: disable=R0201
    def __get_plugin_output(self, argv, process_dict, method):
        """
        This method helps us get the output from the container execute
        and all other commands that rely on container execute such as ps.
        Because of how the container execute method works, we need some
        modifications and cannot use the get_output method of the parent
        class PluginTestBase.
        """
        try:
            process_dict["output"] = method(argv)
        except Exception as exception:
            raise CLIException("Error executing command in container: {error}"
                               .format(error=exception))
        return

    # pylint: disable=R0201
    def __output_retrieve(self, argv, method):
        """
        Helper method that calls the method __get_plugin_output
        as a seperate process and returns the output given by those
        plugins.
        """
        manager = Manager()
        # pylint: disable=E1101
        process_dict = manager.dict()
        process = Process(target=self.__get_plugin_output,
                          args=(argv, process_dict, method,))
        try:
            process.start()
            process.join()
        except Exception as exception:
            self.fail("Error getting execute output: {error}"
                      .format(error=exception))

        return process_dict["output"].strip()

    def __verify_root(self):
        """
        Verify that this test is being run by a root user.
        """
        if os.geteuid() != 0:
            raise unittest.SkipTest("Must be be root for this test!")

    def test_list(self):
        """
        Tests container list.
        We test the list command by checking if the container information
        returned is the same as the /containers endpoint of the agent. We
        also bring down the container and check that the result is empty.
        """
        if not self.setup_success:
            raise unittest.SkipTest("Error setting up cluster in test setup:"
                                    " {log}".format(log=self.error_log))

        # Get container info from from agent to validate list output
        try:
            container_info = hit_endpoint('127.0.0.1:5051', '/containers')
        except Exception as exception:
            self.fail("Could not get /containers from agent node: {error}"
                      .format(error=exception))

        container_plugin = mesos.plugins.container.main.Container(self.config)
        # Now we proced to check if the fields parsed are correct
        list_argv = {"--addr" : "127.0.0.1:5051"}
        try:
            list_output = PluginTestBase.get_output(container_plugin.list,
                                                    list_argv)
        except Exception as exception:
            self.fail("Could not get list output from agent node: {error}"
                      .format(error=exception))

        # Make sure that the return response is a json list with 1 container
        self.assertEqual(type(container_info), list)
        self.assertEqual(len(container_info), 1)
        # Verify the list output has only two lines: header and container info
        try:
            list_output = list_output.split('\n')
        except Exception as exception:
            self.fail("Could not split list table: {error}"
                      .format(error=exception))

        self.assertEqual(len(list_output), 2)
        try:
            list_output = list_output[1].split()
            self.assertEqual(list_output[0], container_info[0]["container_id"])
            self.assertEqual(list_output[1], container_info[0]["framework_id"])
            self.assertEqual(list_output[2], container_info[0]["executor_id"])
        except Exception as exception:
            self.fail("Could not verify list table info: {error}"
                      .format(error=exception))

        # Bring down a container and check list output
        try:
            self.kill_exec()
            self.check_exec_down()
        except Exception as exception:
            self.fail("Could not bring down task: {error}"
                      .format(error=exception))

        # Make sure the list output has only one line: the header
        try:
            list_output = PluginTestBase.get_output(container_plugin.list,
                                                    list_argv)
            list_output = list_output.split('\n')
        except Exception as exception:
            self.fail("Could not get list output from agent node: {error}"
                      .format(error=exception))

        self.assertEqual(len(list_output), 1)
        # Bring back a task for further tests
        try:
            self.launch_exec()
        except Exception as exception:
            self.fail("Could not launch task: {error}"
                      .format(error=exception))

    def test_execute(self):
        """
        Tests container execute.
        We check that the container pid is present in the /containers endpoint
        since it is required for us to execute in a container. We then execute
        within the container and get the output of the 'ps' command, checking
        if the number of processes returned matches our expectation of the
        number of processes within the container.
        """
        if not self.setup_success:
            raise unittest.SkipTest("Error setting up cluster in test setup:"
                                    " {log}".format(log=self.error_log))

        self.__verify_root()
        # Get container id from from agent to execute command in
        try:
            container_info = hit_endpoint('127.0.0.1:5051', '/containers')
            self.assertEqual(len(container_info), 1)
            container_id = container_info[0]["container_id"]
        except Exception as exception:
            self.fail("Could not get container id from agent node: {error}"
                      .format(error=exception))
        # Make sure the executor_id is present in the /containers endpoint
        pid_exists = False
        if "executor_pid" in container_info[0]["status"]:
            pid_exists = True

        self.assertTrue(pid_exists)
        container_plugin = mesos.plugins.container.main.Container(self.config)
        execute_argv = {"--addr" : "127.0.0.1:5051",
                        "<container-id>" : container_id,
                        "<command>" : ["ps", "-ax"],
                        "Record" : True}

        execute_output = self.__output_retrieve(execute_argv,
                                                container_plugin.execute)
        self.assertEqual(len(execute_output.split('\n')), 5)

    def test_logs(self):
        """
        Tests container logs.
        We read the stdout logs from the filesystem and compare it with the
        result we get from our container logs function.
        """
        if not self.setup_success:
            raise unittest.SkipTest("Error setting up cluster in test setup:"
                                    " {log}".format(log=self.error_log))

        # We need the agent id from its /state id for upcoming assertions
        try:
            agent_state = hit_endpoint('127.0.0.1:5051', '/state')
        except Exception as exception:
            self.fail("Could not get agent state info: {error}"
                      .format(error=exception))

        # We read the stout file from the fs and compare with cat output
        try:
            exec_info = hit_endpoint('127.0.0.1:5051', '/containers')
        except Exception as exception:
            self.fail("Could not get /containers from agent: {error}"
                      .format(error=exception))

        if os.geteuid() == 0:
            work_dir = self.sudo_agent_dir
        else:
            work_dir = self.agent_dir

        path_stdout = ('{_dir}/slaves/{agent_id}/frameworks'
                       '/{frame_id}/executors/{exec_id}/runs/{cont_id}/stdout'
                       .format(_dir=work_dir,
                               agent_id=agent_state["id"],
                               frame_id=exec_info[0]["framework_id"],
                               exec_id=exec_info[0]["executor_id"],
                               cont_id=exec_info[0]["container_id"]))

        try:
            with open(path_stdout, 'r') as f:
                real_output = f.read()
        except Exception as exception:
            self.fail("Could not open stdout file: {error}"
                      .format(error=exception))

        # Get container id from from agent to execute command in
        try:
            container_info = hit_endpoint('127.0.0.1:5051', '/containers')
            self.assertEqual(len(container_info), 1)
            container_id = container_info[0]["container_id"]
        except Exception as exception:
            self.fail("Could not get container id from agent node: {error}"
                      .format(error=exception))

        container_argv = {"--addr" : "127.0.0.1:5051",
                          "<container-id>" : container_id,
                          "--no-stderr" : True,
                          "--no-stdout" : False}
        container_plugin = mesos.plugins.container.main.Container(self.config)
        try:
            test_output = PluginTestBase.get_output(container_plugin.logs,
                                                    container_argv)
        except Exception as exception:
            self.fail("Could not get logs of the container: {error}"
                      .format(error=exception))

        self.assertEqual(test_output, real_output.strip())

    def test_ps(self):
        """
        Tests container ps.
        The testing details of this mirror the execute testing because
        container ps is essentially a wrapper around container execute.
        """
        if not self.setup_success:
            raise unittest.SkipTest("Error setting up cluster in test setup:"
                                    " {log}".format(log=self.error_log))

        self.__verify_root()
        # Get container id from from agent to execute command in
        try:
            container_info = hit_endpoint('127.0.0.1:5051', '/containers')
            self.assertEqual(len(container_info), 1)
            container_id = container_info[0]["container_id"]
        except Exception as exception:
            self.fail("Could not get container id from agent node: {error}"
                      .format(error=exception))

        container_plugin = mesos.plugins.container.main.Container(self.config)
        ps_argv = {"--addr" : "127.0.0.1:5051",
                   "<container-id>" : container_id,
                   "Record" : True}

        ps_output = self.__output_retrieve(ps_argv, container_plugin.ps)
        self.assertEqual(len(ps_output.split('\n')), 5)

    def test_stats(self):
        """
        Tests container stats.
        We check to see if we can get an output from the ncurses and verify
        the proper heading is being used. We can't compare top because system
        top output may differ.
        """
        if not self.setup_success:
            raise unittest.SkipTest("Error setting up cluster in test setup:"
                                    " {log}".format(log=self.error_log))

        self.__verify_root()
        # Get container id from from agent to execute command in
        try:
            container_info = hit_endpoint('127.0.0.1:5051', '/containers')
            self.assertEqual(len(container_info), 1)
            container_id = [container_info[0]["container_id"]]
        except Exception as exception:
            self.fail("Could not get container id from agent node: {error}"
                      .format(error=exception))

        container_plugin = mesos.plugins.container.main.Container(self.config)
        stats_argv = {"--addr" : "127.0.0.1:5051",
                      "<container-id>" : container_id,
                      "Record" : True}

        stats_output = self.__output_retrieve(stats_argv,
                                              container_plugin.stats)
        partial_output = "====== Container stats for {container} ======". \
                         format(container=container_id[0])

        self.assertTrue(partial_output in stats_output)

    # pylint: disable=W0511
    # TODO: Implement this once container images has been re-implemented
    def test_images(self):
        """
        Tests container images.
        (Detailed description of test will be added once test is introduced)
        """
        pass
