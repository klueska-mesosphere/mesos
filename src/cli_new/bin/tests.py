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
This is the main executable of the mesos-cli unit tests.
"""

import unittest
from termcolor import colored

import config

from mesos.plugins.agent.tests import Test_AgentPlugin
from mesos.plugins.cluster.tests import Test_ClusterPlugin
from mesos.plugins.container.tests import Test_ContainerPlugin

if __name__ == '__main__':
    print colored("Running the Mesos CLI unit tests", "yellow")

    Test_ClusterPlugin.config = config
    Test_AgentPlugin.config = config
    Test_ContainerPlugin.config = config

    unittest.main(verbosity=2)
