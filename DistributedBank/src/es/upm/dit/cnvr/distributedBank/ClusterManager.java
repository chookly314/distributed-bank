package es.upm.dit.cnvr.distributedBank;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import es.upm.diy.cnvr.distributedBank.persistence.ClientDBImpl;
import es.upm.diy.cnvr.distributedBank.persistence.DBConn;

public class ClusterManager {

	private static Logger logger = Logger.getLogger(ClusterManager.class);

	private List<Integer> znodeList;
	// Process znode id
	private int znodeID;
	// Leader sequential znode number in the "members" tree
	private int leader;
	private ZooKeeper zk;
	// This variable stores the number of processes that have been tried to start
	// but have not been confirmed yet.
	private int pendingProcessesToStart = 0;

	public ClusterManager() {
		// TODO:
		// - if it is the first process - continue as normal
		// - if not - check it there is a node in /state
		// --- yes: get the state from there
		// --- no: that must be because the cluster is starting
		
		
		// Create a Zookeeper session
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
					String.format("Error creating the session between the ClusterManager and Zookeeper", e.toString()));
		}

		// Add the process to /members
		if (zk != null) {
			try {
				// Create a directory, if it is not created
				String response = new String();
				Stat s = zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, false);
				if (s == null) {
					response = zk.create(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, new byte[0],
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					logger.info(String.format("{} directory created.",
							ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT));
				}

				// Create a znode and get the id
				String znodeIDString = zk.create(
						ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT
								+ ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_PREFIX,
						new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				znodeIDString = znodeIDString.replace(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT
						+ ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_PREFIX, "");
				logger.debug(String.format("Created zNode member id: {}", znodeIDString));
				znodeID = Integer.valueOf(znodeIDString);

				// Update the list with the current znodes
				znodeList = new ArrayList<Integer>();
				znodeList = getZnodeList();
				if (znodeList == null) {
					logger.error("Error getting current znode list while creating a ClusterManager.");
					throw new NullPointerException(
							"znodeList is null and that should not be possible if things work as expected.");
				}
				// Get current leader
				leaderElection();
			} catch (KeeperException e) {
				logger.error(String.format("Exception while adding the process to {}: {}",
						ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, e.toString()));
				return;
			} catch (InterruptedException e) {
				logger.error(String.format("Interrupted exception raised while adding the process to {}: {}",
						ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, e.toString()));
			}
		}

		// 
	}

	private synchronized List<Integer> getZnodeList() {
		Stat s = null;
		try {
			s = zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, false);
		} catch (KeeperException | InterruptedException e) {
			logger.error(String.format("Error getting the members tree: {}", e.toString()));
		}

		if (s != null) {
			List<String> znodeListString = null;
			try {
				znodeListString = zk.getChildren(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, false, s);
			} catch (KeeperException e) {
				logger.error(String.format("Error getting members znodes tree: {}", e.toString()));
			} catch (InterruptedException e) {
				logger.error(String.format("Error getting members znodes tree: {}", e.toString()));
			}

			// Parse and convert the list to int
			List<Integer> newZnodeList = new ArrayList<Integer>();
			for (String znode : znodeListString) {
				znode = znode.replace(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT
						+ ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_PREFIX, "");
				newZnodeList.add(Integer.valueOf(znode));
			}
			return newZnodeList;
		} else {
			return null;
		}
	}

	private synchronized void leaderElection() {
		try {
			Stat s = zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, false);

			// Get current leader
			List<String> list = zk.getChildren(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, false, s);
			String leaderString = getNewLeader(list);
			// The previous method returns something with the format: member-0000000001
			leaderString = leaderString.replace(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_PREFIX.substring(1,
					ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_PREFIX.length()), "");
			logger.info(String.format("The current cluster leader is {}.", leaderString));
			leader = Integer.valueOf(leaderString);

			// Set a watcher in /members
			zk.getChildren(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, watcherMember, s);
		} catch (Exception e) {
			logger.error(String.format("Error electing leader: {}", e.toString()));
		}
	}

	private synchronized String getNewLeader(List<String> list) {
		Long leader = 0L;
		String leaderStr = "";
		Pattern p = Pattern.compile("(?<=" + ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_PREFIX + ")\\d{10}");
		boolean firstTime = true;

		for (Iterator iterator = list.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
			Matcher m = p.matcher(string);
			if (firstTime) {
				if (m.find()) {
					leader = Long.parseLong(m.group(0));
					leaderStr = string;
				}
			}
			firstTime = false;
			if (m.find() && Long.parseLong(m.group(0)) < leader) {
				leader = Long.parseLong(m.group(0));
				leaderStr = string;
			}
		}
		return leaderStr;
	}

	private synchronized void handleZnodesUpdate() {
		// Check if this process is the leader
		boolean isLeader = (znodeID == leader) ? true : false;
		// Check whether the event was a creation
		List<Integer> updatedZnodeList = getZnodeList();
		if (updatedZnodeList == null) {
			logger.error("Error getting current znode list while handling an update.");
			throw new NullPointerException(
					"znodeList is null and that should not be possible if things work as expected.");
		}
		boolean creation = (updatedZnodeList.size() > znodeList.size()) ? true : false;

		// This variable will help us to determine when is the update process completed
		// by this process
		boolean updateHandlePending = true;
		while (updateHandlePending) {
			// Case: znode deleted and this process is the leader -> undo the current
			// operation (if exists), dump the state of the system for the new process and create it
			if (!creation && isLeader) {
				pendingProcessesToStart++;
				// Get /operations znode and remove it if exists
				
				
				// Get /locks znodes and remove them
				List<Integer> locksToDelete = getLocks();
				
				
				//
				setUpNewServer();
				// TODO: continue if there is something more here...
				// ...
				updateHandlePending = false;
				// TODO: Think about the possible failure cases...

				// Case: znode deleted and this process is not the leader - check if the
				// leader is still up:
				// - yes: do nothing
				// - no: get the new leader and start again
			} else {
				if (!(updatedZnodeList.contains(leader))) {
					leaderElection();
					updateHandlePending = true;
				} else {
					updateHandlePending = false;
				}
			}
		}
		// Set a watcher in /members
		zk.getChildren(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, watcherMember, s);
	}

	private synchronized void setUpNewServer() {
		// 1. Create the znode with the dump of the database - taken from
		// http://www.java2s.com/Code/Java/File-Input-Output/Convertobjecttobytearrayandconvertbytearraytoobject.htm
		DBConn db = new DBConn();
		HashMap<Integer, BankClient> dbDump = db.getDatabase();
		// Convert Map to byte array
		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(byteOut);
		out.writeObject(dbDump);
		out.flush();
		byte[] bytes = byteOut.toByteArray();
		zk.create(ConfigurationParameters.ZOOKEEPER_TREE_STATE_ROOT, bytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

		// 2. Create the new process
		String command = ConfigurationParameters.SERVER_CREATION_MACOS;
		StringBuffer output = new StringBuffer();
		Process p;
		try {
			p = Runtime.getRuntime().exec(command);
			p.waitFor();
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

			String line = "";
			while ((line = reader.readLine()) != null) {
				output.append(line + "\n");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return output.toString();

	}

	// TODO: get the current locks -- decide if this class handles the incomplete
	// update or if UpdateManager does it
	private synchronized List<Integer> getLocks() {

	}

	// *** Watchers ***

	private Watcher sessionWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			logger.info(String.format("ClusterManager Zookeeper session created: {}.", e.toString()));
			notify();
		}
	};

	// Notified when the number of children in the members branch is updated
	private Watcher watcherMember = new Watcher() {
		public void process(WatchedEvent event) {
			handleZnodesUpdate();
		}
	};

}
