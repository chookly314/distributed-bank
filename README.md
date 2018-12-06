# Dependencies

"Jar" files used in the project are in the "JarFiles" directory and listed below:
 * __Zookeeper:__ we will use Zookeeper for distributed communications between Java processes.
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