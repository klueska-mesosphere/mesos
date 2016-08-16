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

from mesos.PluginTestBase import PluginTestBase

# pylint: disable=C0103
class Test_ClusterPlugin(PluginTestBase):
    """
    Test class for the cluster plugin. Inherits from PluginTestBase.
    A cluster will be spawned by the setup methods in the base class which
    will be used to check commands against.
    """

    def test_execute(self):
        """
        Tests cluster execute.
        (Detailed description of test will be added once test is introduced)
        """
        pass

    def test_cat(self):
        """
        Tests cluster cat.
        (Detailed description of test will be added once test is introduced)
        """
        pass

    def test_ps(self):
        """
        Tests cluster ps.
        (Detailed description of test will be added once test is introduced)
        """
        pass
