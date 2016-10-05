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
The container plugin
"""

import ctypes
import curses
import glob
import os
import subprocess
import sys
import time

from multiprocessing import Process, Manager

import mesos.util

from mesos.exceptions import CLIException
from mesos.plugins import PluginBase
from mesos.util import Table


PLUGIN_CLASS = "Container"
PLUGIN_NAME = "container"

VERSION = "Mesos CLI Container Plugin 1.0"

SHORT_HELP = "Container specific commands for the Mesos CLI"


class Container(PluginBase):
    """
    Container plugin class. Inherits from Plugins Base.
    """

    COMMANDS = {
        "list" : {
            "arguments" : [],
            "flags" : {
                "--addr=<addr>" : "IP and port of agent " + \
                                  "[default: {agent_ip}:{agent_port}]"
            },
            "short_help" : "List all running containers on an agent",
            "long_help"  : """\
                Lists all running containers on an agent.
                """
        },
        "execute" : {
            "arguments" : ["<container-id>", "<command>..."],
            "flags" : {
                "--addr=<addr>" : "IP and port of agent " + \
                                  "[default: {agent_ip}:{agent_port}]"
            },
            "short_help" : "Execute a command within the specified container",
            "long_help"  : """\
                Runs the provided command within the container specified
                by <container-id>. Only supports the Mesos Containerizer.
                """
        },
        "logs" : {
            "arguments" : ["<container-id>"],
            "flags" : {
                "--addr=<addr>" : "IP and port of agent " + \
                                  "[default: {agent_ip}:{agent_port}]",
                "--no-stdout" : "Do not print stdout",
                "--no-stderr" : "Do not print stderr"
            },
            "short_help" : "Show logs",
            "long_help"  : """\
                Show stdout/stderr logs of a container
                Note: To view logs the --work_dir flag in the agent
                must be an absolute path.
                """
        },
        "ps" : {
            "arguments" : ["<container-id>"],
            "flags" : {
                "--addr=<addr>" : "IP and port of agent " + \
                                  "[default: {agent_ip}:{agent_port}]"
            },
            "short_help" : "Show running processes of a container",
            "long_help"  : """\
                Show the processes running inside of a container.
                """
        },
        "stats" : {
            "arguments" : ["<container-id>..."],
            "flags" : {
                "--addr=<addr>" : "IP and port of agent " + \
                                  "[default: {agent_ip}:{agent_port}]"
            },
            "short_help" : "Show status for one or more Containers",
            "long_help"  : """\
                Show various statistics of running containers.
                Inputting multiple ID's will output the container
                statistics for all those containers.
                """
        },
        "images" : {
            "arguments" : [],
            "flags" : {
                "--addr=<addr>" : "IP and port of agent " + \
                                  "[default: {agent_ip}:{agent_port}]"
            },
            "short_help" : "Lists container images",
            "long_help"  : """\
                List images present in the Docker/Appc image store on
                the agent.
                """
        }
    }

    # pylint: disable=R0201
    def __verify_root(self):
        """
        Verify that this command is being executed by the root user.
        """
        if os.geteuid() != 0:
            raise CLIException("Unable to run command as non-root user:"
                               " Consider running with 'sudo'")

    # pylint: disable=R0201
    def __verify_linux(self):
        """
        Verify that this command is being executed on a Linux machine.
        """
        if sys.platform != "linux2":
            raise CLIException("Unable to run command on non-linux system")

    def __nsenter(self, pid):
        """
        Enter a process namespace.
        """
        libc = ctypes.CDLL("libc.so.6")
        namespaces = ["ipc", "uts", "net", "pid", "mnt"]

        for namespace in namespaces:
            path = ("/proc/{pid}/ns/{namespace}"
                    .format(pid=pid, namespace=namespace))
            try:
                file_d = open(path)
            except Exception as exception:
                raise CLIException("Unable to open file '{path}': {error}"
                                   .format(path=path,
                                           error=exception))

            if libc.setns(file_d.fileno(), 0) != 0:
                raise CLIException("Failed to mount '{namespace}' namespace"
                                   .format(namespace=namespace))

    def __check_remote(self, addr):
        """
        Checks if call is for local agent or should we treat as a remote agent.
        """
        addr = addr.split(":")[0]
        try:
            process = subprocess.Popen("ifconfig | grep 'inet '", shell=True,
                                       stdout=subprocess.PIPE)
            output = process.communicate()[0].split('\n')
        except Exception as exception:
            raise CLIException("Could not get set of IP's: {error}"
                               .format(error=exception))

        ip_addr = []
        for line in output:
            if len(line) != 0:
                ip_addr.append(line.strip().split(' ')[1].split(":")[1])

        if addr in ip_addr:
            return False

        return True

    def __remote_command(self, addr, command):
        """
        Executes command on remote node.
        """
        ssh_keys = self.config.SSH_KEYS
        target_ip = addr.split(":")[0]
        try:
            if target_ip in ssh_keys:
                subprocess.call(["ssh", "-i", ssh_keys[target_ip], "-tt", "-o",
                                 "LogLevel=QUIET", target_ip, command])
            else:
                subprocess.call(["ssh", "-tt", "-o", "LogLevel=QUIET",
                                 target_ip, command])
        except Exception as exception:
            raise CLIException("Could not SSH onto remote machine: {error}"
                               .format(error=exception))

        return

    def __get_container(self, addr, container_id):
        """
        Retreives the full container id from a partial container id.
        """
        try:
            containers = mesos.util.hit_endpoint(addr, "/containers")
        except Exception as exception:
            raise CLIException("Could not read from '/containers'"
                               " endpoint: {error}"
                               .format(error=exception))

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

    # pylint: disable=W0511
    # TODO(haris): Reimplement this function to parse the
    # `storedImages` file properly.
    def __parse_images_file(self, path):
        """
        Parse container images from the `storedImages` file.
        The current implementation of this function parses a binary file
        as a text file to extract the names of the images from it!
        """
        try:
            with open(path) as f:
                contents = f.read()
        except Exception as exception:
            raise CLIException("Error opening file '{path}': {error}"
                               .format(path=path, error=exception))

        result = ""
        previous = False
        lines = contents.split('\n')
        for line in lines:
            line = "".join(c for c in line if curses.ascii.isprint(c))
            if '@' in line and not previous:
                result += line.split('@')[0] + "\n"
                previous = True
            else:
                previous = False

        return result

    def list(self, argv):
        """
        Lists container information of an agent.
        """
        try:
            containers = mesos.util.hit_endpoint(argv["--addr"], "/containers")
        except Exception as exception:
            raise CLIException("Could not read from '/containers'"
                               " endpoint: {error}"
                               .format(error=exception))

        if len(containers) == 0:
            print "There are no containers running on this Agent"
            return

        try:
            table = Table(["Container ID", "Framework ID", "Executor"])
            for container in containers:
                table.add_row([container["container_id"],
                               container["framework_id"],
                               container["executor_id"]])
        except Exception as exception:
            raise CLIException("Unable to build table of containers: {error}"
                               .format(error=exception))

        print str(table)

    def execute(self, argv):
        """
        Executes a command within a container. Gets the pid
        of a container from the agent and calls nsenter on its
        namepaces. Works only for the mesos containerizer.
        """
        try:
            if self.__check_remote(argv["--addr"]):
                command = ("sudo mesos container execute {cid} {command}"
                           .format(cid=argv["<container-id>"],
                                   command=" ".join(argv["<command>"])))
                self.__remote_command(argv["--addr"], command)
                return
        except Exception as exception:
            raise CLIException(("Could not check/execute remote invocation:"
                                " {error}".format(error=exception)))

        try:
            self.__verify_linux()
            self.__verify_root()
        except Exception as exception:
            raise CLIException("Could not run command: {error}"
                               .format(error=exception))

        try:
            container = self.__get_container(argv["--addr"],
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

        def enter_container():
            """
            Logic to actually enter a container for a newly executed command.
            Entering the container involves adding the pid of the command to
            all of the cgroups associated with the container as well as
            entering all of its associated namespaces.
            """

            try:
                cgroups = glob.glob("/sys/fs/cgroup/*/mesos/{container}/tasks"
                                    .format(container=argv["<container-id>"]))
            except Exception as exception:
                raise CLIException("Unable to glob cgroup '{cgroup}' for"
                                   " container '{container}': {error}"
                                   .format(cgroup="cgroup",
                                           container=argv["<container-id>"],
                                           error=exception))

            for cgroup in cgroups:
                try:
                    with open(cgroup, 'a') as f:
                        f.write(str(os.getpid()))
                except Exception as exception:
                    raise CLIException("Unable to enter cgroup '{cgroup}' for"
                                       " container '{container}': {error}"
                                       .format(cgroup="cgroup",
                                               container=argv["<container-id>"],
                                               error=exception))

            try:
                self.__nsenter(pid)
            except Exception as exception:
                raise CLIException("Unable to nsenter on pid '{pid}' for"
                                   " container '{container}': {error}"
                                   .format(container=container["container_id"],
                                           pid=pid,
                                           error=exception))

        try:
            # The option to just return the output helps us in testing
            if "Record" in argv and argv["Record"]:
                process = subprocess.Popen(
                    argv["<command>"],
                    preexec_fn=enter_container,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE)

                return "".join(process.communicate())
            else:
                process = subprocess.Popen(
                    argv["<command>"],
                    preexec_fn=enter_container)

                process.communicate()
        except Exception as exception:
            raise CLIException("Unable to execute command '{command}' for"
                               " container '{container}': {error}"
                               .format(command=" ".join(argv["<command>"]),
                                       container=container["container_id"],
                                       error=exception))

        # We ignore cases where it is normal
        # to exit a program via <ctrl-C>.
        except KeyboardInterrupt:
            pass

    def logs(self, argv):
        """
        Reads stdout/stderr logs of a container.
        """
        try:
            state = mesos.util.hit_endpoint(argv["--addr"], "/state")
        except Exception as exception:
            raise CLIException("Unable to read from '/state' endpoint: {error}"
                               .format(error=exception))

        try:
            executors = [executor
                         for framework in state["frameworks"]
                         for executor in framework["executors"]
                         if (executor["container"]
                             .startswith(argv["<container-id>"]))]
        except Exception as exception:
            raise CLIException("Unable to index into state matched"
                               " from the '/state' endpoint: {error}"
                               .format(error=exception))

        if len(executors) == 0:
            raise CLIException("Container ID '{id}' not found"
                               .format(id=argv["<container-id>"]))

        if len(executors) > 1:
            raise CLIException("Container ID '{id}' not unique enough"
                               .format(id=argv["<container-id>"]))

        if not argv["--no-stdout"]:
            try:
                stdout_file = os.path.join(executors[0]["directory"], "stdout")
            except Exception as exception:
                raise CLIException("Unable to construct path to"
                                   " 'stdout' file: {error}"
                                   .format(error=exception))
            try:
                for line in mesos.util.read_file(argv["--addr"], stdout_file):
                    print line
            except Exception as exception:
                raise CLIException("Unable to read from 'stdout' file: {error}"
                                   .format(error=exception))

        if not argv["--no-stderr"]:
            print "=" * 20
            try:
                stderr_file = os.path.join(executors[0]["directory"], "stderr")
            except Exception as exception:
                raise CLIException("Unable to construct path to"
                                   " 'stderr' file: {error}"
                                   .format(error=exception))
            try:
                for line in mesos.util.read_file(argv["--addr"], stderr_file):
                    print line
            except Exception as exception:
                raise CLIException("Unable to read from 'stderr' file: {error}"
                                   .format(error=exception))

    # pylint: disable=C0103
    def ps(self, argv):
        """
        Displays running processes within a contianer.
        """
        argv["<command>"] = ["ps", "-ax"]

        try:
            # The option to just return the output helps us in testing
            if "Record" in argv and argv["Record"]:
                return self.execute(argv)
            else:
                self.execute(argv)

        except Exception as exception:
            raise CLIException("Unable to execute: {error}"
                               .format(error=exception))

    # pylint: disable=R0912,R0914,R0915
    def stats(self, argv):
        """
        Displays statistics of one or multiple containers
        Uses the 'execute' command to retrieve container
        information and displays using ncurses.
        """
        try:
            self.__verify_linux()
            self.__verify_root()
        except Exception as exception:
            raise CLIException("Could not run command: {error}"
                               .format(error=exception))

        containers = argv["<container-id>"]

        pids = {}
        for container in containers:
            try:
                container = self.__get_container(argv["--addr"], container)
            except Exception as exception:
                raise CLIException("Could not retrieve container"
                                   " '{container}': {error}"
                                   .format(container=container,
                                           error=exception))

            try:
                pid = self.__get_pid(container)
            except Exception as exception:
                raise CLIException("Unable to get pid of container"
                                   " '{container}': {error}"
                                   .format(container=container["container_id"],
                                           error=exception))

            pids[container["container_id"]] = pid

        manager = Manager()
        # pylint: disable=E1101
        process_data = manager.dict()

        def get_container_status(process_data):
            """
            We spawn a new subprocess inside our container `pid`
            namespace and run `top` to gather system statistics and
            print the information to the user. We continuously run this
            process once every second until the user hits <ctrl-C>.
            """
            try:
                self.__nsenter(process_data["pid"])
            except Exception as exception:
                sys.exit("Error in subprocess:"
                         " Unable to nsenter: {error}"
                         .format(error=exception))
            except KeyboardInterrupt:
                pass

            command = ["top", "-b", "-d1", "-n1"]

            try:
                process_data["output"] = subprocess.check_output(command)
            except Exception as exception:
                sys.exit("Error in subprocess:"
                         " Unable to run 'top': {error}"
                         .format(error=exception))
            except KeyboardInterrupt:
                pass

        try:
            # We use `curses` to display the container statistics.
            try:
                stdscr = curses.initscr()
            except Exception as exception:
                raise CLIException("Unable to initialize curses screen: {error}"
                                   .format(error=exception))

            # Loop until <Ctrl-C> is pressed.
            last_output = ""

            while True:
                output = ""

                for container, pid in pids.iteritems():
                    try:
                        process_data['container'] = container
                        process_data['pid'] = pid
                        process_data["output"] = ""

                        process = Process(target=get_container_status,
                                          args=(process_data,))

                        process.start()
                        process.join()
                    except Exception as exception:
                        raise CLIException("Unable to run subprocess"
                                           " inside container '{container}'"
                                           " for pid '{pid}: {error}"
                                           .format(container=container,
                                                   pid=pid,
                                                   error=exception))

                    if not process_data["output"]:
                        raise CLIException("Unable to obtain output from"
                                           " running subprocess inside"
                                           " container '{container}'"
                                           " for pid '{pid}'"
                                           .format(container=container,
                                                   pid=pid))

                    # Parse relevant information from the top output
                    output += ("====== Container stats for {container} ======\n"
                               .format(container=container))
                    lines = process_data['output'].split('\n')
                    lines.pop(0)
                    for line in lines:
                        if line == '':
                            break
                        else:
                            output += line + "\n"
                    output += "\n\n"

                # Print the output to the `curses` screen.
                try:
                    # The option to just return the output helps us in testing
                    if "Record" in argv and argv["Record"]:
                        curses.endwin()
                        return output
                    else:
                        stdscr.addstr(0, 0, output)
                except Exception as exception:
                    raise CLIException("Unable to add output to the"
                                       " curses screen: {error}"
                                       .format(error=exception))

                try:
                    stdscr.refresh()
                except Exception as exception:
                    raise CLIException("Unable to refresh the"
                                       " curses screen: {error}"
                                       .format(error=exception))

                last_output = output

                # Sleep for 1 second.
                try:
                    time.sleep(1)
                except Exception as exception:
                    raise CLIException("Unable to sleep: {error}"
                                       .format(error=exception))
        except KeyboardInterrupt:
            try:
                curses.endwin()
                os.system("clear")
            except Exception:
                pass

        print last_output

    def images(self, argv):
        """
        Gets container images present in the image store of the node.
        """
        try:
            self.__verify_root()
        except Exception as exception:
            raise CLIException("Could not run command: {error}"
                               .format(error=exception))

        try:
            flags = mesos.util.hit_endpoint(argv["--addr"], "/flags")
        except Exception as exception:
            raise CLIException("Unable to read from '/flags' endpoint: {error}"
                               .format(error=exception))

        # List the images in the Docker image store.
        try:
            docker_store = os.path.join(flags["flags"]["docker_store_dir"],
                                        "storedImages")
        except Exception as exception:
            raise CLIException("Unable to construct path to Docker"
                               " 'storedImages' file: {error}"
                               .format(error=exception))

        if os.path.exists(docker_store):
            try:
                output = self.__parse_images_file(docker_store)
            except Exception as exception:
                raise CLIException("Unable to parse 'storedImages' file: "
                                   .format(error=exception))
            print "Docker Image Store:"
            print output if output else "No images found!"
        else:
            print "No Images present in Docker Store!"

        # List the images in the Appc image store.
        try:
            docker_store = os.path.join(flags["flags"]["appc_store_dir"],
                                        "storedImages")
        except Exception as exception:
            raise CLIException("Unable to construct path to Appc"
                               " 'storedImages' file: {error}"
                               .format(error=exception))

        if os.path.exists(docker_store):
            try:
                output = self.__parse_images_file(docker_store)
            except Exception as exception:
                raise CLIException("Unable to parse 'storedImages' file: "
                                   .format(error=exception))
            print "Appc Image Store:"
            print output if output else "No images found!"
        else:
            print "No Images present in Appc Store!"
