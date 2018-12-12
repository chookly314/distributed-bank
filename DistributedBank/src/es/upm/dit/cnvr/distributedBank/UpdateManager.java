package es.upm.dit.cnvr.distributedBank;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
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
	private boolean cancelPendingOperation;

	private final static Object lock = new Object();

	public UpdateManager(BankCore bankcore, ZooKeeper zk, ClusterManager cm, ClientDB database) {

		this.bankcore = bankcore;
		this.zk = zk;
		this.cm = cm;
		this.database = database;
		cancelPendingOperation = false;

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
				logger.debug("Created operations znode");
				Stat state = zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, operationsWatcher);
				logger.debug("Setting watcher in operations znode");
			} catch (Exception e) {
				logger.error("Error submitting operation znode");
				logger.error(e.toString());
			}

			// Wait until all other nodes delete their lock
			List<String> locks = this.getLocks();
			while ((!locks.isEmpty()) && (!containsOnlyLeader(locks))) {
				synchronized (lock) {
					try {
						lock.wait();
					} catch (InterruptedException e) {
						logger.error(String.format("Interrupted Exception in wait %s", e));
					}
				}
				locks = this.getLocks();
				logger.debug(String.format("/locks now is %s", locks.toString()));
				logger.debug(locks.isEmpty());
			}
		
			
			// Delete my own lock (leader)
			try {
				zk.delete(ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT
						+ ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_PREFIX + Integer.toString(cm.getZnodeId()), -1);
			    logger.debug("Deleting my lock (leader's): "+ ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT
						+ ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_PREFIX + Integer.toString(cm.getZnodeId()));
			} catch (Exception e) {
				logger.info("While deleting lock: "+ e.toString());
			}

			// Check if operation has been deleted (handles node failures)
			try {
				logger.debug("Checking operation node in case it's been deleted");
				byte[] currentOperationContent = zk.getData(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, false,
						zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, false));
				logger.debug(currentOperationContent.toString());
				if (Arrays.equals(currentOperationContent, new byte[0])) {
					logger.debug("Operation is empty. Aborting persist.");
					cancelPendingOperation = true;
				}
			} catch (Exception e) {
				logger.info("Error emptying operation znode");
				logger.info(e.toString());
			}
			

			if (!cancelPendingOperation) {
				persistOperation(operation);
				cancelPendingOperation = false;
			}
			bankcore.updating = false;
			return;

		} else {

				logger.info("Calling processOperation on a non-leader node");
				System.out.println("You are trying to modify data on a non-leader node.");
				System.out.println("The leader is "+cm.getLeader()+", please send him the operation");
				
	}
	}

	private synchronized void operationsZnodeChanged() {
		if (!bankcore.updating) {
			bankcore.updating = true;
			String currentLeader = ClusterManager.leaderStr;
			//logger.info("BankCore updating TRUE!!!!");
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

			// Sleep for debugging - test case 2 and 3
			logger.debug("I am sleeping, KILL ME");
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			// Delete my lock
			try {
				zk.delete(ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT
						+ ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_PREFIX + Integer.toString(cm.getZnodeId()), -1);
				logger.debug("Deleting lock: "+ ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT
						+ ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_PREFIX + Integer.toString(cm.getZnodeId()));
			} catch (Exception e) {
				logger.error("Exception deleting lock");
			}
			List<String> locks = this.getLocks();
			while (!locks.isEmpty()) {
				// If the leader process fails, abort
				try {
					if (zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT + "/" + currentLeader, false) == null) {
						return;
					}
				} catch (KeeperException e) {
					logger.error(String.format("Error trying to know if the leader znode still exists while updating. Error: %s", e));
				} catch (InterruptedException e) {
					logger.error(String.format("Error trying to know if the leader znode still exists while updating. Error: %s", e));
				}
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
			}
			
			logger.debug("Every node got the operation, persisting the change");
			
			
			// Check if operations node has been emptied, in case the watcher notification was missed
			try {
				logger.debug("Checking operation node in case it's been deleted");
				byte[] currentOperationContent = zk.getData(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, false,
						zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, false));
				logger.debug(currentOperationContent.toString());
				//if (currentOperationContent.equals()) {
				if (Arrays.equals(currentOperationContent, new byte[0])) {
					logger.debug("Operation is empty. Aborting persist.");
					cancelPendingOperation = true;
				}
			} catch (Exception e) {
				logger.info("Error emptying operation znode");
				logger.info(e.toString());
			}
			
			// Every node got the operation, persist it
			if (!cancelPendingOperation) {
				persistOperation(operation);
				cancelPendingOperation = false;
			}

			//Set a new watcher on the operations node
			try {
				zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, operationsWatcher);
			} catch (Exception e) {
				logger.error(e.toString());
			}
			
			bankcore.updating = false;
			logger.info("BankCore updating FALSE!!!!");

		} else {
			try {
				byte[] data = zk.getData(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, operationsWatcher,
						zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, false));
				logger.info("Contents of operation: "+data.toString());
				//if (data.equals(new byte[0])) {
				  if (Arrays.equals(data, new byte[0])) {
					logger.info("Operation node was deleted and triggered a watcher. Undoing operation");
					cancelPendingOperation = true;
				} else {
					logger.error("Operation node changed while on another update. This should not happen");
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
	}

	public synchronized List<String> getLocks() {
		List<String> locks = null;
		try {
			locks = zk.getChildren(ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT, locksWatcher);
//			logger.debug("Current locks are: "+locks.toString());
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
			logger.debug("There is only one lock and it is not the leader's:");
			logger.debug(locks.get(0));
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
			logger.debug("Triggered locksWatcher");
			numberOfLocksChanged();
		}
	};

}
