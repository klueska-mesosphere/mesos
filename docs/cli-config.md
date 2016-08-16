---
title: Apache Mesos - Mesos CLI Config
layout: documentation
---

# Mesos CLI Config

The Mesos CLI uses a config file to get various information about the state of the cluster among other things. This config file can be changed prior to building mesos or via the use of envrionment variables.

The config file contains the following:

	AGENT_IP
	AGENT_PORT
	
These two fields allow the CLI to connect to a default agent. If the `--addr` flag is not specified along with a command that interacts with an agent, the CLI will attempt to contact the agent specified by these variables. The `MESOS_CLI_AGENT_IP` and `MESOS_CLI_AGENT_PORT` environment variables can be used to change these configurations. Both fields must be strings.

	MASTER_IP
	MASTER_PORT
	
These two fields allow the CLI to connect to a default master. If the `--addr` flag is not specified along with a command that interacts with an master, the CLI will attempt to contact the master specified by these variables. The `MESOS_CLI_MASTER_IP` and `MESOS_CLI_MASTER_PORT` environment variables can be used to change these configurations. Both fields must be strings.

	PLUGINS

`Plugins` is a list whose entries specify a absolute path to the plugin directory. The environment variable `MESOS_CLI_PLUGINS` can be used to specify additional plugin paths. Note that setting this variable will combine paths with existing plugin paths, not replace them.

	EXECUTABLE_DIR

`EXECUTABLE_DIR` is the absolute path to the directory that contains executables/scripts for your CLI commands to run if an external executable/script is needed. If left as an empty string, the executables/scripts are assumed to be in your path. The path must end with a `/`. The environment variable `MESOS_EXECUTABLE_DIR` can be used to change these configurations.

It is also possible to specify a JSON file with custom configurations and use the `MESOS_CLI_CONFIG_FILE` environment variable to point towards the absolute path of that file. The CLI will automatically replace whatever config variables defined in the JSON. Please note the order of presedence is: 
	
	Inidividual Enviroment Variables (MESOS_AGENT_IP etc) > MESOS_CLI_CONFIG_FILE > Config files edits
	
The JSON config must be a dictionary mapping of the config variable name (all lowercase) to its value. A sample config file can be:

	{
        "master_ip" : "10.10.0.31",
        "agent_ip" : "10.10.0.30",
        "master_port" : "5050",
        "agent_port" : "5051"
	}


