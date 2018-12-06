package es.upm.dit.cnvr.distributedBank;

public  class ConfigurationParameters {
	
	public static int  CLUSTER_GOAL_SIZE = 3;
	public static int ZOOKEEPER_SESSION_TIMEOUT = 5000;
	private static String ZOOKEEPER_TREE_SEPARATOR = "-";
	public static String ZOOKEEPER_TREE_MEMBERS_ROOT = "/members";
	public static String ZOOKEEPER_TREE_MEMBERS_PREFIX = "/member" + ZOOKEEPER_TREE_SEPARATOR;
	public static String ZOOKEEPER_TREE_STATE_ROOT = "/state"; 
		
}
