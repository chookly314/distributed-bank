# Dependencies

"Jar" files used in the project are in the "JarFiles" directory and listed below:
 * __Zookeeper:__ we will use Zookeeper for distributed communications between Java processes.
 * __Apache Commons Lang:__ Used to easily serialize/deserialize objects
 * __log4j:__ used to handle all the logging needs. To use it just:
	 * Create an object:
					``` 
		private  static  Logger  logger  = Logger.getLogger(MyClass.class);
			``` 

	+ Set the log level and output your message:

		```
		logger.info("Have fun with log4j!");
		```
		
		The available log levels are (): debug, info, warn, error, fatal.

# Running Zookeeper

A Docker compose file is included to create a local Zookeeper cluster. From the project root directory, run (with "sudo" if required):
```
docker-compose -f zookeeper-compose.yaml up
```
This will create three containerized Zookeeper nodes, listening on ports 2181, 2182 and 2183.

# How to execute

1. Start the Zk cluster.

2. Run "start.sh".

# Important notes

* The project directory structure should be the same as here.

* Support for macOS and Linux only.