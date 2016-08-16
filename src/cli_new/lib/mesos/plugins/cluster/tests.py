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
Testing for the Cluster Plugin
"""

import os
import subprocess
import time
import unittest

import mesos.plugins.cluster.main
from mesos.util import hit_endpoint

from mesos.PluginTestBase import PluginTestBase

# pylint: disable=C0103
class Test_ClusterPlugin(PluginTestBase):
    """
    Test class for the cluster plugin. Inherits from PluginTestBase.
    A cluster will be spawned by the setup methods in the base class which
    will be used to check commands against.
    """
    config = None

    def test_execute(self):
        """
        Test cluster execute.
        This test calls the external mesos-execute script and
        checks to see if the container for the task has been
        listed in the /containers endpoint of the agent.
        """
        flags = ["--master=127.0.0.1:5050", "--name=cluster-test",
                 "--command='/bin/bash'"]

        try:
            exec_proc = subprocess.Popen(("exec {path}mesos-execute"
                                          " {flags} > /dev/null 2>&1 ")
                                         .format(flags=' '.join(flags),
                                                 path=self.config. \
                                                         EXECUTABLE_DIR),
                                         shell=True)
        except Exception as exception:
            self.fail("Could not execute task on cluster: {error}"
                      .format(error=exception))

        time.sleep(1)
        try:
            containers = hit_endpoint('127.0.0.1:5051', '/containers')
        except Exception as exception:
            self.fail("Could not get container info: {error}"
                      .format(error=exception))

        self.assertEquals(len(containers), 2)
        exec_proc.kill()
        exec_proc.wait()

    def test_cat(self):
        """
        Test cluster cat.
        This test checks the stdout file of the task and compares it to
        the actual stdout file present in the filesystem. We also attempt
        to cat the wrong file/task id and expect an error.
        """
        if not self.setup_success:
            raise unittest.SkipTest("Error setting up cluster in test setup:"
                                    " {log}".format(log=self.error_log))

        cluster = mesos.plugins.cluster.main.Cluster(self.config)
        # We need the agent id from its /state id as its usefull for assertion
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

        file_path = ('{_dir}/slaves/{agent_id}/frameworks/{frame_id}/executors/'
                     '{exec_id}/runs/{cont_id}/{_file}')
        path_stdout = file_path. \
                        format(_dir=work_dir,
                               agent_id=agent_state["id"],
                               frame_id=exec_info[0]["framework_id"],
                               exec_id=exec_info[0]["executor_id"],
                               cont_id=exec_info[0]["container_id"],
                               _file='stdout')
        try:
            with open(path_stdout, 'r') as f:
                real_output = f.read()
        except Exception as exception:
            self.fail("Could not open stdout file: {error}"
                      .format(error=exception))

        cluster_argv = {"--addr" : "127.0.0.1:5050",
                        "<framework-ID>" : exec_info[0]["framework_id"],
                        "<task-ID>" : exec_info[0]["executor_id"],
                        "<file>" : "stdout"}

        try:
            test_output = PluginTestBase.get_output(cluster.cat, cluster_argv)
        except Exception as exception:
            self.fail("Could not cat file with cluster cat: {error}"
                      .format(error=exception))

        self.assertEqual(test_output, real_output.strip())

        # Try to cat wrong file name and expect an error
        cluster_argv["<file>"] = "wrongfile"
        with self.assertRaises(Exception) as exception:
            test_output = PluginTestBase.get_output(cluster.cat, cluster_argv)

        # cat the wrong task id and expect an error
        cluster_argv["<file>"] = "stdout"
        cluster_argv["<task-ID>"] = "-123"
        with self.assertRaises(Exception) as exception:
            test_output = PluginTestBase.get_output(cluster.cat, cluster_argv)

    def test_ps(self):
        """
        Test cluster ps.
        This test will hit the agent /state endpoint and use it to
        check whether the ps information is correct or not. It also
        brings down the task to expect an empty table.
        """
        if not self.setup_success:
            raise unittest.SkipTest("Error setting up cluster in test setup:"
                                    " {log}".format(log=self.error_log))

        cluster = mesos.plugins.cluster.main.Cluster(self.config)
        # We need the /state endpoint as its usefull for assertion
        try:
            agent_state = hit_endpoint('127.0.0.1:5051', '/state')
        except Exception as exception:
            self.fail("Could not get agent state info: {error}"
                      .format(error=exception))

        cluster_argv = {"--addr" : "127.0.0.1:5050"}
        try:
            test_output = PluginTestBase.get_output(cluster.ps, cluster_argv)
        except Exception as exception:
            self.fail("Could not perform cluster ps: {error}"
                      .format(error=exception))
        # Table should have only two entries: header and entry
        ps_table = test_output.split('\n')
        self.assertEqual(len(ps_table), 2)
        # We now check fields for correctness
        row = ps_table[1].split()
        self.assertEqual(row[0], agent_state['frameworks'][0]['user'])
        self.assertEqual(row[1]+' '+row[2], 'mesos-execute instance')
        self.assertEqual(row[3],
                         agent_state['frameworks'][0]['executors'][0]['id'])
        self.assertEqual(row[4], agent_state['hostname'])
        # If we bring down a task, cluster ps must return empty
        try:
            self.kill_exec()
            self.check_exec_down()
        except Exception as exception:
            self.fail("Could not bring down task: {error}"
                      .format(error=exception))

        cluster_argv = {"--addr" : "127.0.0.1:5050"}
        try:
            test_output = PluginTestBase.get_output(cluster.ps, cluster_argv)
        except Exception as exception:
            self.fail("Could not perform cluster ps: {error}"
                      .format(error=exception))

        # Table should have only one entry: header
        ps_table = test_output.split('\n')
        self.assertEqual(len(ps_table), 1)
        # Bring back a task for further tests
        try:
            self.launch_exec()
        except Exception as exception:
            self.fail("Could not launch task: {error}"
                      .format(error=exception))
