package es.upm.dit.cnvr.distributedBank;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class BankCore {
	
	private static Logger logger = Logger.getLogger(ClusterManager.class);
	private ZooKeeper zk;
	private boolean leader;
	
	public BankCore () {
		this.leader = true;
		
		//We will create a Zookeeper session for test purposes. This session should be obtained from other classes
		try {
			if (zk == null) {
				zk = new ZooKeeper(ZookeeperServersEnum.getRandomServer(),
						ConfigurationParameters.ZOOKEEPER_SESSION_TIMEOUT, sessionWatcher);
				try {
					// Wait for creating the session. Use the object lock
					wait();
				} catch (Exception e) {
				}
			}
		} catch (Exception e) {
			logger.error(
					String.format("Error creating the session between the UpdateManager and Zookeeper", e.toString()));
		}
		
		UpdateManager updatemanager = new UpdateManager(this, zk);

	}
	
	public static void main(String[] args) {
		
		BankCore bankcore = new BankCore();
		
	}
	
	public boolean isLeader() {
		return this.leader;
	}
	
	// *** Watchers ***

	private Watcher sessionWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			logger.info(String.format("ClusterManager Zookeeper session created: {}.", e.toString()));
			notify();
		}
	};
	
}
