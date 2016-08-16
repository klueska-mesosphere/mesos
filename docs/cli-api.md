---
title: Apache Mesos - Mesos CLI Reference
layout: documentation
---

# Mesos CLI Reference

The Mesos CLI has a plethora of plugins and their commands that can be used. The current plugins are:

* Container
* Agent
* Cluster

The details of these plugins and their commands will be outlined below. The general format for CLI commands is as follows:

	$ mesos <plugin> <command> [options] [arguments]
	
##Container

The container plugin provides commands to interact with containers within Mesos. The container plugin commands are as follow:

###execute

`execute` allows us to enter any container namespace and run a provided command within the container namespace. This only works on the Mesos Containerizer. For the Docker containerizer, please use the docker CLI.

	$ mesos container execute [--addr=<addr:port>] [--all] <container> <command>
	
	<container>		The container ID in which to execute the command
	<command>		The command to perform within the container
	
	--addr	The address of the agent, by default it picks the address from the config file.
	--all	Run this command over all agents in the cluster and consolidate all output

###list
`list` allows us to list all running containers on the agent.

	$$ mesos container ps [--all] [--addr=<addr:port>]

	--addr	The address of the agent, by default it picks the address from the config file.
	--all	Run this command over all agents in the cluster and consolidate all output


###logs
`logs` will print out the stdout/stderr logs of a specific container.

	$ mesos container logs [--addr=<addr:port>] [--all] [--no-stdout] [--no-stderr] <container>

	<container>		The container ID in which to execute the command

	--addr			The address of the agent, by default it picks the address from the config file.
	--all			Run this command over all agents in the cluster and consolidate all output
	--no-stdout		If specified, will not print out stdout.
	--no-stderr		If specified, will not print out stderr.

###images
`images` will print out all the images stored in the agent’s docker_images and appc_images directory. 

	$ mesos container images [--addr=<addr:port>] [--all]
	
	--addr			The address of the agent, by default it picks the address from the config file.
	--all-agents	Run this command over all agents in the cluster and consolidate all output
	

##Agent
The agent plugin provides commands to interact with Mesos agents. The Agent plugin commands are as follow:

###ping
`ping` checks to see if an Agent is healthy or not.

	$ mesos agent ping [--addr=<addr:port>] [--all]

	--addr	The address of the agent, by default it picks the address from the config file.
	--all	Run this command over all agents in the cluster and consolidate all output

	
###state
`state` allows a user to retrieve the `/state` endpoint of an agent and also allows a convenient way to parse information from the endpoint.

	$ mesos agent state [--addr=<addr:port>] [--all] <list of fields>

	--addr	The address of the agent, by default it picks the address from the config file.
	--all	Run this command over all agents in the cluster and consolidate all output
	
	<list of fields> refers to one or more fields to parse from the /state endpoint.
	A field may be parsed from the JSON to get its value. The format is <field_name> or 	<array_index> separated by a '.' Multiple fields can be parsed from the same command.

	For example, getting just the work_dir and the checkpoint information from the state would 	result in the following invocation:

	$ mesos agent state flags.work_dir frameworks.0.checkpoint
	
##Cluster
The cluster plugin allows us to interact with the mesos cluster as a whole. The Cluster plugin commands are as follow:

###list
`list` will return all agents and masters running on this cluster.

	$ mesos cluster list [--addr=<addr:port>]

	--addr	The address of the master, by default it picks the address from the config file.

###execute
`execute` will launch a task on the Mesos Cluster. It's input arguments and flags are the same as `mesos execute` from the previous CLI.

###cat
`cat` outputs the entire contents of a file present in the specified task and framework sandbox.

	$mesos cluster cat [--addr=<addr:port>] <framework> <task> <file>
	
	<framework>		Framework ID
	<task>			Task ID
	<file>			File name

	--addr	The address of the master, by default it picks the address from the config file.
	
###ps
`ps` lists various information such as Mem, Time and allocated CPU for all frameworks and tasks running on the cluster.

	$ mesos cluster ps [--addr=<addr:port>]

	--addr	The address of the master, by default it picks the address from the config file.



	
	
	
	
	
	