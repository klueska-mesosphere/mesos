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
This is the base class for all plugins. It helps abstract all methods
that need to setup and tear down a cluster before a plugin test begins.
Also contains common methods used across test cases.
"""

import os
import subprocess
import StringIO
import sys
import time
import unittest

import config
from mesos.exceptions import CLIException
from mesos.util import hit_endpoint


class PluginTestBase(unittest.TestCase):
    """
    PluginTestBase class. It inherits from the TestCase class
    """

    agent_dir = '/tmp/cli-test-agent'
    sudo_agent_dir = '/tmp/sudo-cli-test-agent'
    master_dir = '/tmp/cli-master-test'
    master_proc = None
    agent_proc = None
    exec_proc = None
    setup_success = True
    error_log = ''

    @staticmethod
    def get_output(run, argv):
        """
        This helper method redirects the output of plugin
        commands to a string and returns it.
        """
        stdout = sys.stdout
        sys.stdout = StringIO.StringIO()
        try:
            run(argv)
        except Exception as exception:
            # Need to make sure we fix stdout in case something goes wrong
            sys.stdout = stdout
            raise CLIException("Could not get command output: {error}"
                               .format(error=exception))

        sys.stdout.seek(0)
        output = sys.stdout.read().strip()
        sys.stdout = stdout
        return output

    @staticmethod
    def launch_master():
        """
        Responsible for launching a Master node process. It launches the
        process by executing the mesos-master executable whose location
        is specified by the EXECUTABLE_DIR field in the config file.
        """

        if PluginTestBase.master_proc is not None:
            raise CLIException("Master node already spawned")

        if not PluginTestBase.setup_success:
            raise CLIException("Previous setup failed, abort...")

        flags = ["--ip=127.0.0.1",
                 "--work_dir={_dir}".format(_dir=PluginTestBase.master_dir)]
        try:
            master_proc = subprocess.Popen(("exec {path}mesos-master"
                                            " {flags} > /dev/null 2>&1 ")
                                           .format(flags=' '.join(flags),
                                                   path=config.EXECUTABLE_DIR),
                                           shell=True)
        except Exception as exception:
            PluginTestBase.setup_success = False
            raise CLIException("Could not start master node: {error}"
                               .format(error=exception))

        PluginTestBase.master_proc = master_proc

    @staticmethod
    def launch_agent():
        """
        Responsible for launching a Agent node process. It launches the
        process by executing the mesos-agent executable whose location
        is specified by the EXECUTABLE_DIR field in the config file.
        """

        if PluginTestBase.agent_proc is not None:
            raise CLIException("Agent node already spawned")

        if not PluginTestBase.setup_success:
            raise CLIException("Previous setup failed, abort...")

        if os.geteuid() == 0:
            work_dir = PluginTestBase.sudo_agent_dir
        else:
            work_dir = PluginTestBase.agent_dir

        flags = ["--master=127.0.0.1:5050",
                 "--work_dir={_dir}".format(_dir=work_dir)]
        # If we run test as root we need namespace isolation enabled
        if os.geteuid() == 0:
            flags.append("--isolation=namespaces/pid")
        try:
            agent_proc = subprocess.Popen(("exec {path}mesos-agent"
                                           " {flags} > /dev/null 2>&1 ")
                                          .format(flags=' '.join(flags),
                                                  path=config.EXECUTABLE_DIR),
                                          shell=True)
        except Exception as exception:
            PluginTestBase.setup_success = False
            raise CLIException("Could not start agent node: {error}"
                               .format(error=exception))

        PluginTestBase.agent_proc = agent_proc

    @staticmethod
    def launch_exec():
        """
        Responsible for launching a task on a cluster. It launches the
        task by executing the mesos-execute executable whose location
        is specified by the EXECUTABLE_DIR field in the config file.
        """

        flags = ["--master=127.0.0.1:5050", "--name=cluster-test",
                 "--command='/bin/bash'"]

        try:
            exec_proc = subprocess.Popen(("exec {path}mesos-execute"
                                          " {flags} > /dev/null 2>&1 ")
                                         .format(flags=' '.join(flags),
                                                 path=config.EXECUTABLE_DIR),
                                         shell=True)
        except Exception as exception:
            PluginTestBase.setup_success = False
            raise CLIException("Could not execute task on cluster: {error}"
                               .format(error=exception))

        PluginTestBase.exec_proc = exec_proc
        time.sleep(1)

    @staticmethod
    def check_stable():
        """
        It takes some time for the cluster to stabilize, i.e.
        making sure agent is registered with the master. This
        method will make sure we proceed after this has occurred.
        We continuously poll the master, inbetween a small sleep, to check.
        """

        if PluginTestBase.master_proc is None:
            raise CLIException("No master in cluster!")

        if PluginTestBase.agent_proc is None:
            raise CLIException("No agent in cluster!")

        agent_connected = False
        agents = None
        # We keep a track of time so we dont loop indefinitely
        start_time = time.time()
        timeout = 5
        while not agent_connected:
            time.sleep(0.05)
            try:
                agents = hit_endpoint('127.0.0.1:5050', '/slaves')
            except Exception:
                pass

            if agents is not None:
                if len(agents['slaves']) == 1:
                    agent_connected = True
                    continue
            # We've been probing till timeout, things are probably wrong
            if time.time() - start_time > timeout:
                PluginTestBase.setup_success = False
                raise CLIException("Cluster could not stabilize within {sec}"
                                   " seconds".format(sec=str(timeout)))

    @staticmethod
    def check_exec_down():
        """
        Makes sure that if we kill a task, we proceed once its container
        has been removed. We continuously check the containers endpoint
        of the agent inbetween a small sleep.
        """

        container_gone = False
        containers = None
        # We keep a track of time so we dont loop indefinitely
        start_time = time.time()
        timeout = 5
        while not container_gone:
            time.sleep(0.05)
            try:
                containers = hit_endpoint('127.0.0.1:5051', '/containers')
            except Exception as exception:
                raise CLIException("Could not get containers: {error}"
                                   .format(error=exception))

            if containers is not None:
                if len(containers) == 0:
                    container_gone = True
                    continue
            # We've been probing till timeout, things are probably wrong
            if time.time() - start_time > timeout:
                raise CLIException("Container did not go down within {sec}"
                                   " seconds".format(sec=str(timeout)))

    @staticmethod
    def kill_master():
        """
        Brings down a previously spawned Master process.
        """

        if PluginTestBase.master_proc is None:
            return

        try:
            PluginTestBase.master_proc.kill()
            PluginTestBase.master_proc.wait()
            PluginTestBase.master_proc = None
        except Exception as exception:
            raise CLIException("Could not terminate master node: {error}"
                               .format(error=exception))

    @staticmethod
    def kill_agent():
        """
        Brings down a previously spawned Agent process.
        """

        if PluginTestBase.agent_proc is None:
            return

        try:
            PluginTestBase.agent_proc.kill()
            PluginTestBase.agent_proc.wait()
            PluginTestBase.agent_proc = None
        except Exception as exception:
            raise CLIException("Could not terminate agent node: {error}"
                               .format(error=exception))
    @staticmethod
    def kill_exec():
        """
        Brings down a previously spawned mesos-execute process.
        """

        if PluginTestBase.exec_proc is None:
            return

        try:
            PluginTestBase.exec_proc.kill()
            PluginTestBase.exec_proc.wait()
            PluginTestBase.exec_proc = None
        except Exception as exception:
            raise CLIException("Could not terminate execute process: {error}"
                               .format(error=exception))

    @classmethod
    def setUpClass(cls):
        """
        This method is invoked prior to each plugins test suite is run.
        It brings up a cluster and makes sure its stabilized before proceeding
        onwards to check commands. If a different setup is required, simply
        override this method in the Plugin's test class.
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

        try:
            cls.launch_exec()
        except Exception as exception:
            cls.error_log += ("Failed to launch task on cluster: {error}"
                              .format(error=exception))

    @classmethod
    def tearDownClass(cls):
        """
        This method is invoked after each plugins test suite is run.
        It brings down a cluster. If a different teardown is required, simply
        override this method in the Plugin's test class.
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

        try:
            cls.kill_exec()
        except Exception as exception:
            raise CLIException("Failed to tear down exec proc: {error}"
                               .format(error=exception))
