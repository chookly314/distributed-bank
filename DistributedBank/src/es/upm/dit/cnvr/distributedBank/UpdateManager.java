package es.upm.dit.cnvr.distributedBank;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import es.upm.dit.cnvr.distributedBank.cluster.ClusterManager;
import es.upm.dit.cnvr.distributedBank.persistence.*;

import org.apache.zookeeper.ZooDefs.Ids;

public class UpdateManager {
	private static Logger logger = Logger.getLogger(UpdateManager.class);
	private ZooKeeper zk;
	private BankCore bankcore;
	private ClusterManager cm;
	private ClientDB database;
	public UpdateManager(BankCore bankcore, ZooKeeper zk, ClusterManager cm, ClientDB database) {
		
		this.bankcore = bankcore;
		this.zk = zk;
		this.cm = cm;
		this.database = database;
		
		logger.debug("Instantiating UpdateManager");

		
		//Create a node for storing operations
		try {
			Stat s = zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, false);
			if (s == null) {
				zk.create(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, new byte[0],
						Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				logger.debug("Creating Operations node");
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
				logger.debug("Create locks node");
			}
		} catch (Exception e)
		{
			logger.error("Error creating locks node");
		}
		
		//Set watchers
		if (!bankcore.isLeader()) {
			try {
				Stat s = zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, operationsWatcher);
				logger.debug("Creating watcher for operations node");
			} catch (Exception e) {
				logger.error("Error setting watcher for operations node");
			} 
		}
		
	}
	
	public void processOperation(Operation operation) {
		if (bankcore.isLeader()) {
			logger.debug("Got processOperation request");
			//Create a lock for every process
			List<Integer> nodeList = cm.getZnodeList();
			for (Integer item:nodeList) {
				try {
					String znodeIDString = zk.create(
							ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT
									+ ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_PREFIX
									+ item.toString(),
							new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					logger.debug("Created lock for"+znodeIDString);
				} catch (Exception e){
					logger.error("Error creating lock for node"+item.toString());
					logger.error(e.toString());
				}
			}
			logger.debug("All locks created");

			//Create an operation znode
			logger.info(operation.toString());
			
			byte[] data = SerializationUtils.serialize(operation);

			try {
				Stat s = zk.setData(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT,
						data, -1);
				logger.debug("Created operation znode");
			} catch (Exception e) {
				logger.error("Error submitting operation znode");
				logger.error(e.toString());
			}
			
			//Wait until all other nodes delete their lock
			List<String> locks = this.getLocks();
			while (!containsOnlyLeader(locks)) {
				try {
					wait();
				} catch (InterruptedException e) {
					logger.error("Interrupted Exception in wait");
				}
				locks = this.getLocks();
			}
			
			//logger.info("victoria");
			
			//Delete my own lock (leader)
			try {
			zk.delete(ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT
					+ ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_PREFIX
					+ Integer.toString(cm.getZnodeId()),-1);
			} catch (Exception e) {
				logger.error("Exception deleting lock");
			}
			
			//Remove operation
			try {
				Stat s = zk.setData(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT,
						new byte[0], -1);
			} catch (Exception e) {
				logger.error("Error emptying operation znode");
				logger.error(e.toString());
			}
			
			persistOperation(operation);
			bankcore.updating = false;
			return;
			
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
				
				//operation = SerializationUtils.deserialize(data);	
				
				/*	Object obj = null;
					ByteArrayInputStream bis = null;
					ObjectInputStream ois = null;
					bis = new ByteArrayInputStream(data);
					ois = new ObjectInputStream(bis);
					obj = ois.readObject();
					operation = (Operation) obj;
					bis.close();
				
				*/
			} catch (Exception e) {
				logger.error("Error getting operation from zookeeper:");
				logger.error(e.toString());
			} /*catch (ClassNotFoundException e) {
				logger.error(String.format("Could not cast from object to HashMap. Error: {}", e));
			} catch (IOException e) {
				logger.error(String.format("Could not read input stram. Error: {}", e));
			} */
			
			//Delete my lock
			try {
				zk.delete(ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT
						+ ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_PREFIX
						+ Integer.toString(cm.getZnodeId()),-1);
				} catch (Exception e) {
					logger.error("Exception deleting lock");
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
			RuntimeException e = new RuntimeException("Operation node changed while on another update");
			logger.error(e.toString());
			throw e;
		}

	}
	
	public void persistOperation(Operation operation) {
		switch (operation.getOperation()) {
		case CREATE:
			ServiceStatusEnum responseCreate = database.create(operation.getClient());
			logger.info(String.format("Response: %s", responseCreate));
			break;
		case READ:
			logger.error("Getting read operation in UpdateManager");
			break;
		case UPDATE:
			ServiceStatusEnum responseUpdate = database.update(operation.getAccountNumber(), operation.getBalance());
			logger.info(String.format("Response: %s", responseUpdate));
			break;
		case DELETE:
			ServiceStatusEnum responseDelete = database.delete(operation.getAccountNumber());
			logger.info(String.format("Response: %s", responseDelete));
			break;
		}
	}
	
	private void numberOfLocksChanged() {
		//Awake this thread
		notify();
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
	
	public boolean containsOnlyLeader(List<String> locks) {
		if (locks.size() != 1) {
			return false;
		} else if (locks.get(0).equals(ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_PREFIX_NO_SLASH + Integer.toString(cm.getLeader()))) {
			return true;
		} else {
			logger.error("There is only one lock and it is not the leader's:");
			logger.error(locks.get(0));
			return false;
		}
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
