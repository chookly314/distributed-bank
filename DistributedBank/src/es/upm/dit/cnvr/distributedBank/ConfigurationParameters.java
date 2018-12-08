package es.upm.dit.cnvr.distributedBank;

public  class ConfigurationParameters {
	
	public static int  CLUSTER_GOAL_SIZE = 3;
	public static int CLUSTER_WATCHDOG_SLEEP_CYCLE = 10; // seconds
	public static int ZOOKEEPER_SESSION_TIMEOUT = 1500;
	public static String ZOOKEEPER_TREE_SEPARATOR = "-";
	public static String ZOOKEEPER_TREE_LOCKS_ROOT = "/locks";
	public static String ZOOKEEPER_TREE_LOCKS_PREFIX = "/lock" + ZOOKEEPER_TREE_SEPARATOR;
	public static String ZOOKEEPER_TREE_LOCKS_PREFIX_NO_SLASH = "lock" + ZOOKEEPER_TREE_SEPARATOR;
	public static String ZOOKEEPER_TREE_MEMBERS_ROOT = "/members";
	public static String ZOOKEEPER_TREE_MEMBERS_PREFIX = "/member" + ZOOKEEPER_TREE_SEPARATOR;
	public static String ZOOKEEPER_TREE_MEMBERS_PREFIX_NO_SLASH = "member" + ZOOKEEPER_TREE_SEPARATOR;
	public static String ZOOKEEPER_TREE_STATE_PREFIX = "/state" + ZOOKEEPER_TREE_SEPARATOR;
	public static String ZOOKEEPER_TREE_OPERATIONS_ROOT = "/operations";
	public static String ZOOKEEPER_TREE_STATE_ROOT = "/state"; 
	public static String ZOOKEEPER_TREE_STATE_PATH = "/state/dbDump"; 
	public static String PROJECT_MAIN_PATH = "es.upm.dit.cnvr.distributedBank";
	public static String PROJECT_WORKING_DIRECTORY = "";
	public static String PROJECT_START_SCRIPT = "start.sh";
	// - macOS
	public static String SERVER_CREATION_PREFIX_MAC = "osascript -e 'tell app \"Terminal\" to do script \"";// + PROJECT_WORKING_DIRECTORY + PROJECT_START_SCRIPT + "\"'";
	public static String SERVER_CREATION_SUFIX_MAC = "\"'";
	// - Linux
	public static String SERVER_CREATION_PREFIX_LINUX = "gnome-terminal -x sh -c \"";
	public static String SERVER_CREATION_SUFIX_LINUX = "\"";
	public static String SERVER_CREATION = "";
	

	
	
//	public static String APPLICATION_DIRECTORY = new File(ConfigurationParameters.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
}
