package es.upm.dit.cnvr.distributedBank;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public enum ZookeeperServersEnum {

	// We will start 2 Zookeeper servers and balance the load between them
	SERVER1(BankCore.zookeeperIpAddress+":2181"), 
	SERVER2(BankCore.zookeeperIpAddress+":2182"), 
	SERVER3(BankCore.zookeeperIpAddress+":2183"); 

	
	private static final List<String> VALUES;

	private final String value;

	static {
		VALUES = new ArrayList<>();
		for (ZookeeperServersEnum server : ZookeeperServersEnum.values()) {
			VALUES.add(server.value);
		}
	}

	private ZookeeperServersEnum(String value) {
	        this.value = value;
	    }

	public static String getRandomServer() {
		Random rand = new Random();
		int i = rand.nextInt(VALUES.size());
		return VALUES.get(i);
	}

}


