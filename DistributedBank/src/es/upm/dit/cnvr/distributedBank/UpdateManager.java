package es.upm.dit.cnvr.distributedBank;

import java.util.List;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import es.upm.dit.cnvr.distributedBank.cluster.ClusterManager;


import org.apache.zookeeper.ZooDefs.Ids;

public class UpdateManager {
	private static Logger logger = Logger.getLogger(UpdateManager.class);
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
		
		//Create a node for locks
		try {
			Stat s = zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT, false);
			if (s == null) {
				zk.create(ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT, new byte[0],
						Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);	
			}
		} catch (Exception e)
		{
			logger.error("Error creating locks node");
		}
		
		//Set watchers
		if (!bankcore.isLeader()) {
			try {
				Stat s = zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, operationsWatcher);
			} catch (Exception e) {
				logger.error("Error setting watcher for operations node");
			} 
		}
		
	}
	
	public ServiceStatus processOperation(Operation operation) {
		if (bankcore.isLeader()) {
			
			return ServiceStatus.OK;
			
		} else {
			RuntimeException e = new RuntimeException("Calling processOperation on a non-leader node");
			logger.error(e.toString());
			throw e;
		}
	}
	
	private void operationsZnodeChanged() {
		if (!bankcore.updating) {
			bankcore.updating = true;
			Operation operation = null;
			//Connect to ZooKeeper and get the operation
			try {
				byte[] data = zk.getData(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, operationsWatcher, zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, false));
				operation = SerializationUtils.deserialize(data);
				//byte[] data = SerializationUtils.serialize(operation);
			} catch (Exception e) {
				logger.error("Error getting operation from zookeeper:");
				logger.error(e.toString());
			}
			
			//Delete my lock
			try {
					//TO-DO
			} catch (Exception e)
			{
				logger.error("Error creating locks node");
			}
			
			List<String> locks = this.getLocks();
			while (!locks.isEmpty()) {
				try {
					wait();
				} catch (InterruptedException e) {
					logger.error("Interrupted Exception in wait");
				}
				locks = this.getLocks();
			}
			
			//Every node got the operation, persist it
			persistOperation(operation);
			bankcore.updating = false;
			
		} else {
			/*TO-DO: I was updating and the watcher was triggered:
				- The operation was deleted
			*/
		}

	}
	
	public void persistOperation(Operation operation) {
		/*switch (operation.getOperation()) {
		case CREATE:
			
			break;
		case READ:
			
			break;
		case UPDATE:
			
			break;
		case DELETE:
		
			break;

		}*/
	}
	
	private void numberOfLocksChanged() {
		//TO-DO
	}
	
	public List<String> getLocks() {
		List<String> locks = null;
		try {
			locks = zk.getChildren(ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT, locksWatcher);
		} catch (Exception e) {
			logger.error("Error in getLocks");
			//Stop everything. Null could mean lack of locks and generate inconsistencies
			System.exit(1);
		}
		return locks;
	}
	
	// *** Watchers ***
	
	// Notified of changes in the operations znode
	private Watcher operationsWatcher = new Watcher() {
		public void process(WatchedEvent event) {
			operationsZnodeChanged();
		}
	};
		
	private Watcher locksWatcher = new Watcher() {
		public void process(WatchedEvent event) {
			numberOfLocksChanged();
		}
	};
		
	
	
	
}
