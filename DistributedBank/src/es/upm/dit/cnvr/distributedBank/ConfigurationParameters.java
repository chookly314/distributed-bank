package es.upm.dit.cnvr.distributedBank;

public  class ConfigurationParameters {
	
	public static int  CLUSTER_GOAL_SIZE = 3;
	public static int CLUSTER_WATCHDOG_SLEEP_CYCLE = 10; // seconds
	public static int ZOOKEEPER_SESSION_TIMEOUT = 5000;
	private static String ZOOKEEPER_TREE_SEPARATOR = "-";
	public static String ZOOKEEPER_TREE_LOCKS_ROOT = "/locks";
	public static String ZOOKEEPER_TREE_MEMBERS_ROOT = "/members";
	public static String ZOOKEEPER_TREE_MEMBERS_PREFIX = "/member" + ZOOKEEPER_TREE_SEPARATOR;
	public static String ZOOKEEPER_TREE_OPERATIONS_ROOT = "/operations";
	public static String ZOOKEEPER_TREE_STATE_ROOT = "/state"; 
	public static String ZOOKEEPER_TREE_STATE_PATH = "/state/dbDump"; 
	public static String PROJECT_MAIN_PATH = "es.upm.dit.cnvr.distributedBank";
	public static String SERVER_CREATION_MACOS = "osascript -e 'tell app \"Terminal\" to do script \"jar es.upm.dit.cnvr.distributedBank.BankCore\"'";
	public static String SERVER_CREATION_LINUX = "gnome-terminal -x sh -c \"jar es.upm.diy.cnvr.distributedBank.BankCore\"";
	//public static String APPLICATION_DIRECTORY = new File(ConfigurationParameters.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
}
