package es.upm.dit.cnvr.distributedBank;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooDefs.Ids;

public class UpdateManager {
	private static Logger logger = Logger.getLogger(ClusterManager.class);
	private ZooKeeper zk;
	private BankCore bankcore;
	public UpdateManager(BankCore bankcore, ZooKeeper zk) {
		
		this.bankcore = bankcore;
		this.zk = zk;
		
		
		//Create a node for storing operations
		try {
			Stat s = zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, false);
			if (s == null) {
				zk.create(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, new byte[0],
						Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);	
			}
		} catch (Exception e)
		{
			logger.error("Error creating operations node");
		}
		
		//Set watcher on that directory if server is not leader
		if (!bankcore.isLeader()) {
			try {
				Stat s = zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, operationsWatcher);
			} catch (Exception e) {
				logger.error("Error setting watcher for operations node");
			} 
		}
		
		
	}
	
	// *** Watchers ***
	
	// Notified of changes in the operations znode
	private Watcher operationsWatcher = new Watcher() {
		public void process(WatchedEvent event) {
			
		}
	};
	
	
}
