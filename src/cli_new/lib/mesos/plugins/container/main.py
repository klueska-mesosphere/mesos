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

import ctypes
import glob
import os
import subprocess
import sys

from multiprocessing import Manager
from multiprocessing import Process

import psutil

import mesos.http as http

from mesos.exceptions import CLIException
from mesos.plugins import PluginBase
from mesos.util import Table
from mesos.util import is_local
from mesos.util import verify_root
from mesos.util import verify_linux


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
        "exec" : {
            "alias": "execute",
            "arguments" : ["<container-id>", "<command>..."],
            "flags" : {
                "--agent=<addr>" : "IP and port of agent " + \
                                   "[default: {agent_ip}:{agent_port}]"
            },
            "short_help" : "Execute a command within the specified container",
            "long_help"  : """\
                Runs the provided command within the container specified
                by <container-id>. Only supports the Mesos Containerizer.
                """
        },
    }

    def __get_container(self, addr, container_id):
        """
        Retreives the full container id from a partial container id.
        """
        try:
            containers = http.get_json(addr, "containers")
        except Exception as exception:
            raise CLIException("Could not open '/containers'"
                               " endpoint at '{addr}': {error}"
                               .format(addr=addr, error=exception))

        try:
            containers = [container for container in containers
                          if container["container_id"].startswith(container_id)]
        except Exception as exception:
            raise CLIException("Unable to index into container matched"
                               " from the '/containers' endpoint: {error}"
                               .format(error=exception))

        if len(containers) == 0:
            raise CLIException("Container ID '{id}' not found"
                               .format(id=container_id))

        if len(containers) > 1:
            raise CLIException("Container ID '{id}' not unique enough"
                               .format(id=container_id))

        return containers[0]

    def __get_pid(self, container):
        """
        Retrieves the PID of a container matched at the `/containers` endpoint.
        """
        try:
            pid = str(container["status"]["executor_pid"])
        except Exception as exception:
            raise CLIException("Unable to index into container matched"
                               " from the '/containers' endpoint: {error}"
                               .format(error=exception))

        return pid

    def __enter_namespaces(self, pid, namespaces=None, exclude=None):
        """
        Enter a process's namespaces.
        """
        if namespaces is None:
            namespaces = ["ipc", "uts", "net", "pid", "mnt"]

        if exclude is None:
            exclude = []

        libc = ctypes.CDLL("libc.so.6")

        for namespace in namespaces:
            # Exclude those namespaces in the `exclude` list.
            if namespace in exclude:
                continue

            # Find the path to the container's namespace.
            # Special case the 'mnt' namespace to workaround MESOS-5727.
            try:
                if namespace == "mnt":
                    path = self.__get_mnt_namespace_path(pid)
                else:
                    path = ("/proc/{pid}/ns/{namespace}"
                            .format(pid=pid, namespace=namespace))
            except Exception as exception:
                raise CLIException("Unable to get path for '{namespace}'"
                                   " namespace with pid '{pid}': {error}"
                                   .format(namespace=namespace,
                                           pid=pid,
                                           error=exception))

            try:
                file_d = open(path)
            except Exception as exception:
                raise CLIException("Unable to open file '{path}': {error}"
                                   .format(path=path,
                                           error=exception))

            if libc.setns(file_d.fileno(), 0) != 0:
                raise CLIException("Failed to mount '{namespace}' namespace"
                                   .format(namespace=namespace))

    def __get_mnt_namespace_path(self, pid):
        """
        Special case the 'mnt' namespace to workaround MESOS-5727.  To
        find the proper `mnt` namespace to enter, We need to
        recursively look at all children of the container's
        init-process and find the first `mnt` namespace that doesn't
        match the mount namespace of the init-process. If we find one,
        we return its path. If we don't find one, we return the path of
        the original init-process's `mnt` namespace.

        In future versions of mesos, the pod-executor will become the
        default executor and we will always have a proper reference to
        the container whose `mnt` namespace we should enter (thereby
        removing the need for this ugly hack).
        """
        path = "/proc/{pid}/ns/mnt".format(pid=pid)

        try:
            parent_inode = os.stat(path).st_ino
        except Exception as exception:
            raise CLIException("Unable to get the parent"
                               " inode for '{path}': {error}"
                               .format(path=path, error=exception))

        try:
            parent = psutil.Process(pid=int(pid))
            children = parent.children(recursive=True)
        except Exception as exception:
            raise CLIException("Unable to get children of '{pid}'"
                               " for finding 'mnt' namespace: {error}"
                               .format(pid=pid, error=exception))

        for child_pid in [c.pid for c in children]:
            child_path = "/proc/{pid}/ns/mnt".format(pid=child_pid)

            try:
                child_inode = os.stat(child_path).st_ino
            except Exception as exception:
                raise CLIException("Unable to get the child"
                                   " inode for '{path}': {error}"
                                   .format(path=child_path, error=exception))

            if parent_inode != child_inode:
                return child_path

        return path

    def __enter_cgroups(self, container):
        """
        Enter a container's cgroups.
        """
        try:
            cgroups = glob.glob("/sys/fs/cgroup/*/mesos/{container}/tasks"
                                .format(container=container))
        except Exception as exception:
            raise CLIException("Unable to glob cgroup '{cgroup}' for"
                               " container '{container}': {error}"
                               .format(cgroup="cgroup",
                                       container=container,
                                       error=exception))

        for cgroup in cgroups:
            try:
                with open(cgroup, 'a') as f:
                    f.write(str(os.getpid()))
            except Exception as exception:
                raise CLIException("Unable to enter cgroup '{cgroup}' for"
                                   " container '{container}': {error}"
                                   .format(cgroup="cgroup",
                                           container=container,
                                           error=exception))

    def __enter_container(self, pid, container, command, redirect_io=False):
        """
        Logic to actually enter a container and execute a command.
        Entering the container involves adding the pid of the command to
        all of the cgroups associated with the container as well as
        entering all of its associated namespaces.
        """

        def __enter_container_helper(shared_state):
            """
            Call this helper from a forked process to safely enter the
            container without corrupting the CLI's cgroup and namespace
            membership.
            """
            try:
                self.__enter_cgroups(container)
            except Exception as exception:
                raise CLIException("Unable to enter cgroups for '{pid}'"
                                   " in container '{container}': {error}"
                                   .format(container=container,
                                           pid=pid,
                                           error=exception))

            try:
                self.__enter_namespaces(pid, namespaces=["pid"])
            except Exception as exception:
                raise CLIException("Unable to enter namespaces for '{pid}'"
                                   " in container '{container}': {error}"
                                   .format(container=container,
                                           pid=pid,
                                           error=exception))

            def enter_remaining_namespaces():
                """
                Helper function to enter all namespaces except the
                `pid` namespace inside the pre-exec command of the
                subprocess (i.e.  after the fork, but before the exec).
                """
                try:
                    self.__enter_namespaces(pid, exclude=["pid"])
                except Exception as exception:
                    raise CLIException("Unable to enter namespaces for '{pid}'"
                                       " in container '{container}': {error}"
                                       .format(container=container,
                                               pid=pid,
                                               error=exception))

            try:
                if shared_state["redirect_io"]:
                    stdin = subprocess.PIPE
                    stdout = subprocess.PIPE
                    stderr = subprocess.STDOUT
                else:
                    stdin = None
                    stdout = sys.stdout.fileno()
                    stderr = sys.stderr.fileno()

                process = subprocess.Popen(
                    command,
                    close_fds=True,
                    env={},
                    stdin=stdin,
                    stdout=stdout,
                    stderr=stderr,
                    preexec_fn=enter_remaining_namespaces)

                shared_state["stdout"], _ = process.communicate()
            except Exception as exception:
                shared_state["exception"] = (
                    CLIException("Unable to execute command '{command}'"
                                 " inside container '{container}': {error}"
                                 .format(command=" ".join(command),
                                         container=container,
                                         error=exception)))

            # We ignore cases where it is normal
            # to exit a program via <ctrl-C>.
            except KeyboardInterrupt:
                pass

        # Fork a new process to run the helper command.
        try:
            shared_state = Manager().dict()
            shared_state["redirect_io"] = redirect_io

            process = Process(
                target=__enter_container_helper,
                args=(shared_state,))

            process.start()
            process.join()
        except Exception as exception:
            raise CLIException("Unable to start wrapper process to enter"
                               " container '{container}' to run command"
                               " '{command}': {error}"
                               .format(container=container,
                                       command=command,
                                       error=exception))

        if "exception" in shared_state:
            raise shared_state["exception"]

        return shared_state["stdout"]

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

    def execute(self, argv, redirect_io=False):
        """
        Executes a command within a container.
        Works only for the mesos containerizer.
        """
        try:
            local = is_local(argv["--agent"])
        except Exception as exception:
            raise CLIException("Failed to check if agent is local: {error}"
                               .format(error=exception))

        if not local:
            raise CLIException("Agent must be running on the local"
                               " machine to run the 'exec' command")

        try:
            verify_linux()
            verify_root()
        except Exception as exception:
            raise CLIException("Failed to run command: {error}"
                               .format(error=exception))

        try:
            container = self.__get_container(argv["--agent"],
                                             argv["<container-id>"])
        except Exception as exception:
            raise CLIException("Could not retrieve container"
                               " '{container}': {error}"
                               .format(container=argv["<container-id>"],
                                       error=exception))

        try:
            pid = self.__get_pid(container)
        except Exception as exception:
            raise CLIException("Could not read the pid of container"
                               " '{container}': {error}"
                               .format(container=container["container_id"],
                                       error=exception))

        try:
            output = self.__enter_container(
                pid,
                container["container_id"],
                argv["<command>"],
                redirect_io)

            if output:
                print output
        except Exception as exception:
            raise CLIException("Unable to enter container"
                               " '{container}': {error}"
                               .format(container=container["container_id"],
                                       error=exception))
