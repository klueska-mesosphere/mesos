---
title: Apache Mesos - Mesos CLI Overview
layout: documentation
---

# Mesos CLI Overview


The Mesos CLI has been remade from the ground up to provide many features for interacting with Mesos. It has also been designed carefully with a plugin architecture that allows a user to easily write their own commands and use them with the existing CLI tool.

As the CLI begins to get introduced within the Mesos codebase itself, it is disabled by default. In order to enable it, please use the `--enable-new-cli` flag with `configure.ac` (Note: This will cause the old CLI to not get built). The CLI is written in Python and the use of Pyinstaller allows us to build it into a single executable with minimal external dependancies.

The following is a overview of the design and architecture of the CLI:

<img src="http://i.imgur.com/AZjEch4.png" width="550" height="225" />

We have a top level mesos script that knows where to locate all the folders pertaining to plugins via a config file. So in this case, the mesos script know that there are three plugins: Container, Agent and Cluster. As arguments are passed to the CLI, the wrapper script uses the first argument to determine which plugin to invoke a command on, and then passes the remaining arguments down to that plugin. Plugins themselves are self contained in a single folder, which provides all of the code necessary to deal with logic for that particular plugin’s commands. For example, all of the code for container related commands can be found in the container plugin folder, all of the code for the agent plugin can be found in the agent plugin folder, etc. Adding a new plugin requires a user to simply create a new folder for that plugin, write code for all of its commands, and make the wrapper script aware of the plugin by editing a simple configuration file.

To provide more clarity, let's see what happens when a user invokes the command:

	$ mesos container execute 123abc /bin/bash

<img src="http://i.imgur.com/Me9EhQj.png" width="550" height="225" />

The above command allows us to enter a container and execute a command. `mesos` refers to the CLI itself and `container` is the plugin whose `execute` command we wish to call. The arguments `123abc` is the id for the container on which we want the `/bin/bash` command to run.

The top-level mesos script will first find the container plugin, then call its execute method, passing along all of its arguments. The execute command will than talk to the local agent, receive information on how to access the PID for container 123abc and finally execute /bin/bash inside it.

We are however not restricted to only interacting with local agents. By specifying a remote address in the `--addr=<addr:port>` flag, we are able to point the command to a remote agent on which to run the command.

Now with the following command, we can execute into a container on another agent:

	$ mesos container execute --addr=10.10.0.1:5051 123abc /bin/bash

<img src="http://i.imgur.com/B1fM3XE.png" width="550" height="225" />

The mesos wrapper script will still look for the container plugin and call the execute command on the local agent just as before (Note: The execute logic will be different here as explained further). However this time the execute command will notice the additional flags and will ssh onto the agent at the specified address and re-execute the command there. The results are then piped back to the machine where the command was originally invoked. On the remote machine, the command is treated the same as explained previously.

Finally, sometimes it makes sense to run a command on all agents simultaneously and then consolidate their results. For this, we provide an additional `--all` flag to consolidate the results from all agents and print them in a nice consumable format. We assume a cluster configuration file is provided to allow these commands to discover the state of the cluster.

So for example, the command below allows to list all running containers on a single agent.

	$ mesos container list

If we wish to list containers on all agents, we can use the following command:

	$ mesos container list --all
	
<img src="http://i.imgur.com/74NhpiC.png" width="700" height="225" />

What happens now is that the CLI will iteratively ask all agents for their list of containers and display the aggregate result.
	






