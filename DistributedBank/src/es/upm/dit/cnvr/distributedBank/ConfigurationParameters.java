package es.upm.dit.cnvr.distributedBank;

public  class ConfigurationParameters {
	
	public static int ZOOKEEPER_SESSION_TIMEOUT = 5000;
	private static String ZOOKEEPER_TREE_SEPARATOR = "-";
	public static String ZOOKEEPER_TREE_MEMBERS_ROOT = "/members";
	public static String ZOOKEEPER_TREE_MEMBERS_PREFIX = "/members" + ZOOKEEPER_TREE_SEPARATOR;
		
}
