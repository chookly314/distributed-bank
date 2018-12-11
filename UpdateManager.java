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
import org.apache.zookeeper.KeeperException;
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

	private final static Object lock = new Object();

	public UpdateManager(BankCore bankcore, ZooKeeper zk, ClusterManager cm, ClientDB database) {

		this.bankcore = bankcore;
		this.zk = zk;
		this.cm = cm;
		this.database = database;

		logger.debug("Instantiating UpdateManager");

		// Create a node for storing operations
		try {
			Stat s = zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, false);
			if (s == null) {
				zk.create(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, new byte[0], Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
				logger.debug("Creating Operations node");
			}
		} catch (Exception e) {
			logger.error("Error creating operations node");
		}

		// Create a node for locks
		try {
			Stat s = zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT, false);
			if (s == null) {
				zk.create(ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT, new byte[0], Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
				logger.debug("Create locks node");
			}
		} catch (Exception e) {
			logger.error("Error creating locks node");
		}

		// Set watchers
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
			bankcore.updating = true;
			// Create a lock for every process
			List<Integer> nodeList = cm.getZnodeList(false);
			for (Integer item : nodeList) {
				try {
					String znodeIDString = zk.create(
							ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT
									+ ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_PREFIX + item.toString(),
							new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					logger.debug("Created lock for" + znodeIDString);
				} catch (Exception e) {
					logger.error("Error creating lock for node" + item.toString());
					logger.error(e.toString());
				}
			}
			logger.debug("All locks created");
			
			// Create an operation znode
			logger.info(operation.toString());

			byte[] data = SerializationUtils.serialize(operation);

			try {
				Stat s = zk.setData(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, data, -1);
				logger.debug("Created operation znode");
			} catch (Exception e) {
				logger.error("Error submitting operation znode");
				logger.error(e.toString());
			}

			// Wait until all other nodes delete their lock
			List<String> locks = this.getLocks();
			while (!containsOnlyLeader(locks)) {
				synchronized (lock) {
					try {
						lock.wait();
					} catch (InterruptedException e) {
						logger.error(String.format("Interrupted Exception in wait %s", e));
					}
				}
//				try {
//					wait();
//					logger.info("Woke up");
//				} catch (InterruptedException e) {
//					logger.error(String.format("Interrupted Exception in wait %s", e));
//				}
				locks = this.getLocks();
				logger.debug(String.format("/locks now is %s", locks.toString()));
			}

			// logger.info("victoria");
			// Uncomment the following block to have control over timing for debugging
            
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			 
			
			// Delete my own lock (leader)
			try {
				zk.delete(ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT
						+ ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_PREFIX + Integer.toString(cm.getZnodeId()), -1);
			    logger.debug("Deleting my lock (leader's): "+ ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT
						+ ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_PREFIX + Integer.toString(cm.getZnodeId()));
			} catch (Exception e) {
				logger.error("Exception deleting lock");
			}

			// Remove operation
			try {
				Stat s = zk.setData(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, new byte[0], -1);
				logger.debug("Deleting operation");
			} catch (Exception e) {
				logger.error("Error emptying operation znode");
				logger.error(e.toString());
			}

			persistOperation(operation);
			bankcore.updating = false;
			return;

		} else {

				logger.info("Calling processOperation on a non-leader node");
				
	}
	}

	private void operationsZnodeChanged() {
		if (!bankcore.updating) {
			bankcore.updating = true;
			logger.info("BankCore updating TRUE!!!!");
			Operation operation = null;
			// Connect to ZooKeeper and get the operation
			try {
				byte[] data = zk.getData(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, false,
						zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, false));

				operation = SerializationUtils.deserialize(data);
				logger.debug("Got operation from Zookeeper: "+operation.toString());
				
			} catch (Exception e) {
				logger.error("Error getting operation from zookeeper:");
				logger.error(e.toString());
			}
			//logger.debug("Point 0");

			// Sleep for debugging
			logger.debug("I am sleeping, KILL ME");
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			//
			
			// Delete my lock
			try {
				zk.delete(ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT
						+ ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_PREFIX + Integer.toString(cm.getZnodeId()), -1);
				logger.debug("Deleting lock: "+ ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT
						+ ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_PREFIX + Integer.toString(cm.getZnodeId()));
			} catch (Exception e) {
				logger.error("Exception deleting lock");
			}
			//logger.debug("Point A");
			List<String> locks = this.getLocks();
			//logger.debug("Point B");
			while (!locks.isEmpty()) {
				//synchronized (lock) {
				/*
					try {
						logger.debug("Waiting for the following locks to be removed: "+locks.toString());
						//lock.wait(5000);
						logger.debug("Nofify received. Proceeding");
					} catch (InterruptedException e) {
						logger.error("Interrupted Exception in wait");
					}
					*/
					locks = this.getLocks();
				//}
			}
			
			logger.debug("Every node got the operation, persisting the change");
			
			// Every node got the operation, persist it
			persistOperation(operation);

			//Set a new watcher on the operations node
			try {
				zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, operationsWatcher);
			} catch (Exception e) {
				logger.error(e.toString());
			}
			
			
			bankcore.updating = false;

		} else {
			try {
				byte[] data = zk.getData(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, operationsWatcher,
						zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, false));
				logger.info("Contents of operation: "+data.toString());
				if (data.equals(new byte[0])) {
					logger.info("Operation node was deleted and triggered a watcher. Setting a new watcher");
				} else {
					RuntimeException e = new RuntimeException("Operation node changed while on another update");
					logger.error(e.toString());
					throw e;
				}
			} catch (Exception e) {
				logger.error(e.toString());
			}
		}

	}

	public void persistOperation(Operation operation) {
		logger.info("Persisting operation");
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
		// Awake this thread
		synchronized (lock) {
		    lock.notifyAll();
		    logger.info("Notified");
		}
//		notify();
	}

	public synchronized List<String> getLocks() {
		List<String> locks = null;
		try {
			locks = zk.getChildren(ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT, locksWatcher);
			logger.debug("Current locks are: "+locks.toString());
		} catch (Exception e) {
			logger.error("Error in getLocks");
			// Stop everything. Null could mean lack of locks and generate inconsistencies
			System.exit(1);
		}
		return locks;
	}

	public boolean containsOnlyLeader(List<String> locks) {
		if (locks.size() != 1) {
			return false;
		} else if (locks.get(0).equals(
				ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_PREFIX_NO_SLASH + Integer.toString(cm.getLeader()))) {
			logger.debug("Locks directory contains only leader's "+locks.get(0).toString());
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
			logger.debug("Triggered operationsWatcher");
			operationsZnodeChanged();
		}
	};

	private Watcher locksWatcher = new Watcher() {
		public void process(WatchedEvent event) {
			logger.debug("Number of locks changed. Watcher triggered.");
			numberOfLocksChanged();
		}
	};

}
