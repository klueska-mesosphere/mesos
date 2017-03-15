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
This module implements a set of classes and helper functions for
building unit tests for the Mesos CLI.
"""

import random
import shutil
import StringIO
import subprocess
import sys
import tempfile
import unittest

import parse

import mesos.http as http

from mesos.exceptions import CLIException
from mesos.util import verify_linux
from mesos.util import verify_root


class CLITestCase(unittest.TestCase):
    """
    Base class for CLI TestCases.
    """
    @classmethod
    def setUpClass(cls):
        print "\n" + str(cls.__name__)

    @staticmethod
    def verify_root():
        """
        Decorator used to verify that the test suite has been launched
        by the root user.
        """
        try:
            verify_root()
            return lambda func: func
        except Exception:
            return unittest.skip("Must be root")

    @staticmethod
    def verify_linux():
        """
        Decorator used to verify that the test suite has been launched
        on a linux machine.
        """
        try:
            verify_linux()
            return lambda func: func
        except Exception:
            return unittest.skip("Must be linux")


class Executable(object):
    """
    This class defines the base class for launching an executable for
    the CLI unit tests. It will be subclassed by (at least) a
    'Master', 'Agent', and 'Task' subclass.
    """
    def __init__(self):
        self.name = ""
        self.executable = ""
        self.flags = {}
        self.proc = None

    def __del__(self):
        if hasattr(self, "proc"):
            self.kill()

    def launch(self):
        """
        Launch 'self.excutable'. We assume it is in the system PATH.
        """
        if self.proc is not None:
            raise CLIException("{name} already launched"
                               .format(name=self.name.capitalize()))

        try:
            flags = ["--{key}={value}".format(key=key, value=value)
                     for key, value in self.flags.iteritems()]

            proc = subprocess.Popen([self.executable] + flags,
                                    stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT)
        except Exception as exception:
            raise CLIException("Failed to launch '{executable}': {error}"
                               .format(executable=self.executable,
                                       error=exception))

        if proc.poll():
            raise CLIException("Failed to launch '{executable}': {error}"
                               .format(executable=self.executable,
                                       error=proc.stdout.read()))

        self.proc = proc

    def kill(self):
        """
        Kills a previously launched executable.
        """
        if self.proc is None:
            return

        try:
            self.proc.kill()
            self.proc.wait()
            self.proc = None
        except Exception as exception:
            raise CLIException("Could not kill {name}: {error}"
                               .format(name=self.name, error=exception))


class Master(Executable):
    """
    This class defines the functions necessary to launch a master in
    the CLI unit tests.
    """
    count = 0

    def __init__(self, flags=None):
        super(Master, self).__init__()

        if Master.count > 0:
            raise CLIException("Creating more than one master"
                               " is not currently possible")

        Master.count += 1

        if flags is None:
            flags = {}

        if "ip" not in flags:
            flags["ip"] = "127.0.0.1"
        if "port" not in flags:
            flags["port"] = "9090"
        if "work_dir" not in flags:
            flags["work_dir"] = tempfile.mkdtemp()

        self.flags = flags
        self.name = "master"
        self.executable = "mesos-master.sh"
        self.addr = "{ip}:{port}".format(ip=flags["ip"], port=flags["port"])

    def __del__(self):
        super(Master, self).__del__()

        if hasattr(self, "flags"):
            shutil.rmtree(self.flags["work_dir"])

        Master.count -= 1


class Agent(Executable):
    """
    This class defines the functions necessary to launch an agent in
    the CLI unit tests.
    """
    count = 0

    def __init__(self, flags=None):
        super(Agent, self).__init__()

        if Agent.count > 0:
            raise CLIException("Creating more than one agent"
                               " is not currently possible")

        Agent.count += 1

        if flags is None:
            flags = {}

        if "ip" not in flags:
            flags["ip"] = "127.0.0.1"
        if "port" not in flags:
            flags["port"] = "9091"
        if "master" not in flags:
            flags["master"] = "127.0.0.1:9090"
        if "systemd_enable_support" not in flags:
            flags["systemd_enable_support"] = "false"
        if "work_dir" not in flags:
            flags["work_dir"] = tempfile.mkdtemp()
        if "runtime_dir" not in flags:
            flags["runtime_dir"] = tempfile.mkdtemp()

        self.flags = flags
        self.name = "agent"
        self.executable = "mesos-agent.sh"
        self.addr = "{ip}:{port}".format(ip=flags["ip"], port=flags["port"])

    def __del__(self):
        super(Agent, self).__del__()

        if hasattr(self, "flags"):
            shutil.rmtree(self.flags["work_dir"])
            shutil.rmtree(self.flags["runtime_dir"])

        Agent.count -= 1

    # pylint: disable=arguments-differ
    def launch(self, timeout=5):
        """
        After starting the agent, we need to make sure it has
        successfully registered with the master before proceeding.
        """
        super(Agent, self).launch()

        try:
            # pylint: disable=missing-docstring
            def single_slave(data):
                return len(data["slaves"]) == 1

            http.get_json(self.flags["master"], "slaves", single_slave, timeout)
        except Exception as exception:
            stdout = ""
            if self.proc.poll():
                stdout = "\n" + self.proc.stdout.read()
                self.proc = None

            raise CLIException("Could not get '/slaves' endpoint as"
                               " JSON with only 1 agent in it: {error}{stdout}"
                               .format(error=exception,
                                       stdout=stdout))

    # pylint: disable=arguments-differ
    def kill(self, timeout=5):
        """
        After killing the agent, we need to make sure it has
        successfully unregistered from the master before proceeding.
        """
        super(Agent, self).kill()

        if self.proc is None:
            return

        try:
            # pylint: disable=missing-docstring
            def no_slaves(data):
                return len(data["slaves"]) == 0

            http.get_json(self.flags["master"], "slaves", no_slaves, timeout)
        except Exception as exception:
            raise CLIException("Could not get '/slaves' endpoint as"
                               " JSON with 0 agents in it: {error}"
                               .format(error=exception))



class Task(Executable):
    """
    This class defines the functions necessary to launch task in
    the CLI unit tests.
    """
    def __init__(self, flags=None):
        super(Task, self).__init__()

        if flags is None:
            flags = {}

        if "master" not in flags:
            flags["master"] = "127.0.0.1:9090"
        if "name" not in flags:
            flags["name"] = "task-{id}".format(id=str(random.random()))
        if "command" not in flags:
            raise CLIException("No command supplied when creating task")

        self.flags = flags
        self.name = "task"
        self.executable = "mesos-execute"

    def __wait_for_containers(self, condition, timeout=5):
        """
        Wait for the one and only agent's '/containers'
        endpoint to return data subject to 'condition'.
        """
        try:
            data = http.get_json(self.flags["master"], "slaves")
        except Exception as exception:
            raise CLIException("Could not get '/slaves'"
                               " endpoint as JSON: {error}"
                               .format(error=exception))

        if len(data["slaves"]) != 1:
            raise CLIException("More than one agent detected when"
                               " reading from '/slaves' endpoint")

        try:
            agent = parse.parse(
                "slave({id})@{addr}",
                data["slaves"][0]["pid"])
        except Exception as exception:
            raise CLIException("Unable to parse agent info: {error}"
                               .format(error=str(exception)))

        try:
            data = http.get_json(
                agent["addr"],
                "containers",
                condition,
                timeout)
        except Exception as exception:
            raise CLIException("Could not get '/containers' endpoint"
                               " as JSON subject to condition: {error}"
                               .format(error=exception))

    # pylint: disable=arguments-differ
    def launch(self, timeout=5):
        """
        After starting the task, we need to make sure its container
        has actually been added to the agent before proceeding.
        """
        super(Task, self).launch()

        try:
            # pylint: disable=missing-docstring
            def container_exists(data):
                for container in data:
                    if container["executor_id"] == self.flags["name"]:
                        return True
                return False

            self.__wait_for_containers(container_exists, timeout)
        except Exception as exception:
            stdout = ""
            if self.proc.poll():
                stdout = "\n" + self.proc.stdout.read()
                self.proc = None

            raise CLIException("Container with name '{name} not"
                               " found after timeout: {error}{stdout}"
                               .format(name=self.flags["name"],
                                       error=exception,
                                       stdout=stdout))

    # pylint: disable=arguments-differ
    def kill(self, timeout=5):
        """
        After killing the task, we need to make sure its container has
        actually been removed from the agent before proceeding.
        """
        super(Task, self).kill()

        if self.proc is None:
            return

        try:
            # pylint: disable=missing-docstring
            def container_does_not_exist(data):
                for container in data:
                    if container["executor_id"] == self.flags["name"]:
                        return False
                return True

            self.__wait_for_containers(container_does_not_exist, timeout)
        except Exception as exception:
            raise CLIException("Container with name '{name}' still"
                               " exists after timeout: {error}"
                               .format(name=self.flags["name"],
                                       error=exception))


def capture_output(command, argv, extra_args=None):
    """
    Redirect the output of a command to a string and return it.
    """
    if not extra_args:
        extra_args = {}

    stdout = sys.stdout
    sys.stdout = StringIO.StringIO()

    try:
        command(argv, **extra_args)
    except Exception as exception:
        # Fix stdout in case something goes wrong
        sys.stdout = stdout
        raise CLIException("Could not get command output: {error}"
                           .format(error=exception))

    sys.stdout.seek(0)
    output = sys.stdout.read().strip()
    sys.stdout = stdout

    return output
