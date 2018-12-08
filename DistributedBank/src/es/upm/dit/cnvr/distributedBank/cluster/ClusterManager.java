package es.upm.dit.cnvr.distributedBank.cluster;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
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

import es.upm.dit.cnvr.distributedBank.BankCore;
import es.upm.dit.cnvr.distributedBank.ConfigurationParameters;
import es.upm.dit.cnvr.distributedBank.persistence.DBConn;

public class ClusterManager {

	private static Logger logger = Logger.getLogger(ClusterManager.class);

	private static ArrayList<Integer> znodeListMembers;
	// Full paths
	private static List<String> znodeListMembersString;
	// Full paths
	private static List<String> znodeListState;
	// Process znode id
	private static int znodeId;
	// Full path
	private static String znodeIdState;
	// Leader sequential znode number in the "members" tree
	private static int leader;
	private ZooKeeper zk;
	// This variable stores the number of processes that have been tried to start
	// but have not been confirmed yet.
	private static int pendingProcessesToStart = 0;
	// This variable will be used only by the watchdog
	private static int nodeCreationConfirmed = 0;
	// Watchdog instance, only used by the leader
//	private static Watchdog watchdog = null;
	// This variable will prevent the system from counting the same znode creation
	// twice
//	boolean memberCreationReceived = false;
	// BankCore instance used to notify when the system is restoring
	private BankCore bankCore;
	// List of servers killed locks to remove once the system fully restored. Full
	// paths
	private static ArrayList<String> pendingLocksToRemove = new ArrayList<String>();
	// Will be true if a new leader is elected during an operation. This will
	// trigger a ops followed by locks znodes deletion
	boolean newLeaderElectedDuringUpdate = false;
	// /state znode id of the last contacted process, this variable is only used by
	// the leader
	private static String lastContactedProcess = "";

	// This constructor will be used only by the watchdog
	protected ClusterManager() {
	}

	public ClusterManager(ZooKeeper zk, BankCore bankCore) throws Exception {
		this.bankCore = bankCore;
		this.zk = zk;
		if (zk != null) {
			// Create /members directory, if it is not created
			boolean membersCreation = createZookeeperDirectory(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT);

			// Create /state directory, if it is not created
			boolean stateCreation = createZookeeperDirectory(ConfigurationParameters.ZOOKEEPER_TREE_STATE_ROOT);

			// Exit if something went wrong
			if (!membersCreation || !stateCreation) {
				logger.debug(String.format("Killing the process because membersCreation is {} and stateCreation is {}",
						membersCreation, stateCreation));
				System.exit(1);
			}

			// Check if there are any members
			znodeListMembers = getZnodeList();
			if (znodeListMembers == null) {
				// This process is the first one, the first leader
				addToMembers();
				leaderElection();
				verifySystemState();
			} else {
				if (znodeListMembers.size() >= ConfigurationParameters.CLUSTER_GOAL_SIZE) {
					logger.debug("The cluster already has its goal servers number. Killing myself.");
					System.exit(1);
				} else {
					// Create the socket and add a node to /state to tell the leader to contact me
					// to synchronize the state
					followerStartUp();
				}
			}

//			// If I am the leader, check the servers number
//			if (znodeId == leader) {
//				if (znodeListMembers.size() == ConfigurationParameters.CLUSTER_GOAL_SIZE) {
//					logger.info(String.format("The cluster has {} servers, as expected.", znodeListMembers.size()));
//				} else {
//					// This case means that the number of servers is lower than expected and that we
//					// are in one
//					// of the following two options:
//					// -> First process created in the system
//					// -> Every other process has died and this is the first one living. In this
//					// case, proceed as in the previous option, because processes are created one by
//					// one.
//					bankCore.setIsRestoring(true);
//					pendingProcessesToStart = ConfigurationParameters.CLUSTER_GOAL_SIZE - znodeListMembers.size();
//					setUpNewServer();
//				}
//			} else {
//				// Get the current system state
//				byte[] databaseState = zk.getData(ConfigurationParameters.ZOOKEEPER_TREE_STATE_PATH, false,
//						zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_STATE_PATH, false));
//				DBConn.createDatabase(databaseState);
//
//				// Remove /state znode
//				zk.delete(ConfigurationParameters.ZOOKEEPER_TREE_STATE_PATH, -1);
//
//				logger.info("This process was properly initialized and is also synced now.");
//			}

		}
	}

	private boolean createZookeeperDirectory(String dir) {
		try {
			String response = new String();
			Stat s = zk.exists(dir, false);
			if (s == null) {
				response = zk.create(dir, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			logger.info(String.format("{} directory created. Response: {}", dir, response));
			return true;
		} catch (KeeperException | InterruptedException e) {
			logger.error(String.format("Could not create Zookeeper {} directory. Error: ", dir, e));
			return false;
		}
	}

	private synchronized void addToMembers() {
		// Create a znode and get the id
		try {
			String znodeIDString = null;
			znodeIDString = zk.create(
					ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT
							+ ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_PREFIX,
					new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			znodeIDString = znodeIDString.replace(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT
					+ ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_PREFIX, "");
			logger.debug(String.format("Created zNode member id: {}", znodeIDString));
			znodeId = Integer.valueOf(znodeIDString);
			zk.getChildren(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, watcherMember);
		} catch (KeeperException e) {
			logger.error(String.format("Error creating a znode in members. Exiting to avoid inconsistencies: {}",
					e.toString()));
			System.exit(1);
		} catch (InterruptedException e) {
			logger.error(String.format("Error creating a znode in members. Exiting to avoid inconsistencies: {}",
					e.toString()));
			System.exit(1);
		}
	}

	public synchronized ArrayList<Integer> getZnodeList() {
		Stat s = null;
		try {
			s = zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, false);
		} catch (KeeperException | InterruptedException e) {
			logger.error(String.format("Error getting the members tree: {}", e.toString()));
		}

		if (s != null) {
			List<String> znodeListString = null;
			try {
				znodeListString = zk.getChildren(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, watcherMember, s);
				znodeListMembersString = znodeListString;
			} catch (KeeperException e) {
				logger.error(String.format("Error getting members znodes tree. Exiting to avoid inconsistencies: {}",
						e.toString()));
				System.exit(1);
			} catch (InterruptedException e) {
				logger.error(String.format("Error getting members znodes tree. Exiting to avoid inconsistencies: {}",
						e.toString()));
				System.exit(1);
			}
			if (znodeListString == null) {
				return null;
			}

			// Parse and convert the list to int
			ArrayList<Integer> newZnodeList = new ArrayList<Integer>();
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

	private synchronized List<String> getZnodeStateList() {
		Stat s = null;
		try {
			s = zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_STATE_ROOT, false);
		} catch (KeeperException | InterruptedException e) {
			logger.error(String.format("Error getting the state tree: {}", e.toString()));
		}

		if (s != null) {
			try {
				return zk.getChildren(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, watcherState, s);
			} catch (KeeperException e) {
				logger.error(String.format("Error getting members znodes tree. Exiting to avoid inconsistencies: {}",
						e.toString()));
				System.exit(1);
			} catch (InterruptedException e) {
				logger.error(String.format("Error getting members znodes tree. Exiting to avoid inconsistencies: {}",
						e.toString()));
				System.exit(1);
			}
		}
		return null;
	}

	// Only accessed by the leader
	private synchronized void verifySystemState() {
		pendingProcessesToStart = ConfigurationParameters.CLUSTER_GOAL_SIZE - znodeListMembers.size();
		if (pendingProcessesToStart > 0) {
			setUpNewServer();
		} else {
			logger.debug(String.format("Restoring process finished. PendingProcessesToStart is {}",
					pendingProcessesToStart));
			// Final state for the restoring process
			// Remove pending processes, it there are any
			for (String process : pendingLocksToRemove) {
				try {
					zk.delete(process, -1);
				} catch (InterruptedException e) {
					logger.error(String.format("Problem removing pending lock {}. Error: {}", process, e));
				} catch (KeeperException e) {
					logger.error(String.format("Problem removing pending lock {}. Error: {}", process, e));
				}
			}
			// Reset pending processes
			pendingLocksToRemove = new ArrayList<String>();
			logger.debug("Reseting pending locks to remove.");

			if (newLeaderElectedDuringUpdate) {
				undoCurrentOperation();
			}
		}
	}

	private synchronized void leaderElection() {
		try {
			Stat s = zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, false);

			// Get current leader
			leader = getLowestId(znodeListMembers);
			if (znodeId == leader) {
				bankCore.setIsLeader(true);
			} else {
				bankCore.setIsLeader(false);
			}

			// A leader won't check again if he is the leader, so we set a /state watcher
			// here and update the znodes list
			znodeListState = getZnodeStateList();

//			// The previous method returns something with the format: member-0000000001
//			leaderString = leaderString.replace(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_PREFIX.substring(1,
//					ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_PREFIX.length()), "");
//			logger.info(String.format("The current cluster leader is {}.", leaderString));
//			leader = Integer.valueOf(leaderString);
//			if (znodeId == leader) {
//				if (watchdog == null) {
//					watchdog = new Watchdog();
//					Thread watchdogThread = new Thread(watchdog, "Watchdog thread");
//					watchdogThread.start();
//					logger.debug("Watchdog thread started.");
//				}
//			}
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

	private synchronized int getLowestId(List<Integer> list) {
		int lowestZnodeId = list.get(0);
		for (int id : list) {
			if (id < lowestZnodeId) {
				lowestZnodeId = id;
			}
		}
		return lowestZnodeId;
	}

	private synchronized void setUpNewServer() {
		createNewProcess();
	}

	private synchronized void handleZnodesUpdate() throws KeeperException, InterruptedException {
		// Get the most recent znodeList (by asking Zookeeper) and set a watcher to
		// avoid the possibility of missing information
		ArrayList<String> oldZnodeListString = new ArrayList<>(znodeListMembersString);
		ArrayList<Integer> updatedZnodeList = getZnodeList();
		// Check if the event was a znode creation
		if (updatedZnodeList == null) {
			logger.error("Error getting current znode list while handling an update.");
			throw new NullPointerException(
					"znodeList is null and that should not be possible if things work as expected.");
		}
//		boolean creation = (updatedZnodeList.size() > znodeListMembers.size()) ? true : false;

//		// Set the system in pending from /state watcher
//		if (creation) {
//			memberCreationReceived = true;
//		}

		// Update the znodeList attribute
//		znodeListMembers = new ArrayList<Integer>(updatedZnodeList);

		// Set the number of pending processes to be started to the difference between
		// the goal and the current cluster size
//		pendingProcessesToStart = ConfigurationParameters.CLUSTER_GOAL_SIZE - znodeListMembers.size();

		// This variable will help us to determine when is the update process completed
		// by this process
		boolean updateHandlePending = true;
		while (updateHandlePending) {
			// Check if this process is the leader
			boolean isLeader = (znodeId == leader) ? true : false;

			if (isLeader) {
				updateHandlePending = false;
				for (String id : oldZnodeListString) {
					if (!(znodeListMembersString.contains(id))) {
						pendingLocksToRemove.add(id);
					}
				}
				verifySystemState();
			} else {
				leaderElection();
				updateHandlePending = false;
				if (znodeId == leader) {
					updateHandlePending = true;
					if (bankCore.isUpdating()) {
						newLeaderElectedDuringUpdate = true;
					}
				}
			}

//			// Case: znode deleted and this process is the leader -> undo the current
//			// operation (if exists), dump the state of the system for the new process and
//			// create it
//			if (!creation && isLeader) {
//				updateHandlePending = false;
//				bankCore.setIsUpdating(true);
//				// Get /operations znode and remove it if exists
//				List<String> operations;
//				try {
//					operations = zk.getChildren(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, false,
//							zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, false));
//					if (operations.size() > 0) {
//						for (String znode : operations) {
//							zk.delete(znode, zk.exists(znode, false).getVersion());
//						}
//					}
//				} catch (KeeperException e) {
//					logger.error(String.format("Could not get the list of znodes in /operations. Error: {}", e));
//				} catch (InterruptedException e) {
//					logger.error(String.format("Could not get the list of znodes in /operations. Error: {}", e));
//				}
//				
//				// Get /locks znodes and delete them
//				try {
//					List<String> locks = getLocks();
//					if (locks.size() > 0) {
//						for (String znode : locks) {
//							zk.delete(znode, zk.exists(znode, false).getVersion());
//						}
//					}
//				} catch (KeeperException e) {
//					logger.error(String.format("Could not get the list of znodes in /locks. Error: {}", e));
//				} catch (InterruptedException e) {
//					logger.error(String.format("Could not get the list of znodes in /locks. Error: {}", e));
//				}
//				
//				// Check if the system is creating a new process:
//				// - yes: do nothing, the system will create a new one if needed once it
//				// finishes the current cycle
//				// - no: trigger a process creation
//				logger.debug("Checking if the creation of a new process has already been triggered.");
//				if (zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_STATE_PATH, false) == null) {
//					setUpNewServer();
//				}
//				
//				// Case: znode deleted and this process is not the leader - check if the
//				// leader is still up:
//				// - no: get the new leader and start again
//				// - yes: do nothing
//			} else if (!creation && !isLeader) {
//				pendingProcessesToStart++;
//				if (!(updatedZnodeList.contains(leader))) {
//					updateHandlePending = true;
//					leaderElection();
//				} else {
//					updateHandlePending = false;
//				}
//			}
		}
	}

	private synchronized void undoCurrentOperation() {
		// Get /operations znode and remove it if exists
		List<String> operations;
		try {
			operations = zk.getChildren(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, false,
					zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_OPERATIONS_ROOT, false));
			if (operations.size() > 0) {
				for (String znode : operations) {
					zk.delete(znode, zk.exists(znode, false).getVersion());
				}
			}
		} catch (KeeperException e) {
			logger.error(String.format("Could not get the list of znodes in /operations. Error: {}", e));
		} catch (InterruptedException e) {
			logger.error(String.format("Could not get the list of znodes in /operations. Error: {}", e));
		}

		// Get /locks znodes and delete them
		try {
			List<String> locks = getLocks();
			if (locks.size() > 0) {
				for (String znode : locks) {
					zk.delete(znode, zk.exists(znode, false).getVersion());
				}
			}
		} catch (KeeperException e) {
			logger.error(String.format("Could not get the list of znodes in /locks. Error: {}", e));
		} catch (InterruptedException e) {
			logger.error(String.format("Could not get the list of znodes in /locks. Error: {}", e));
		}
	}

	private synchronized List<String> getLocks() throws KeeperException, InterruptedException {
		return zk.getChildren(ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT, false,
				zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT, false));
	}

	// Only accessed by the leader
	private synchronized void handleStateUpdate() {
		znodeListState = getZnodeStateList();
		// This method should be called only by the leader, but check it just in case
		if (leader != znodeId) {
			return;
		}

		if (lastContactedProcess.equals("")) {
			if (znodeListState.size() != 0) {
				lastContactedProcess = znodeListState.get(0);
				synchronizeWithProcess(lastContactedProcess);
			}
		} else {
			if ((znodeListState.size() != 0) && (znodeListState.get(0).equals(lastContactedProcess))) {
				return;
			} else if ((znodeListState.size() != 0) && !(znodeListState.get(0).equals(lastContactedProcess))) {
				lastContactedProcess = znodeListState.get(0);
				synchronizeWithProcess(lastContactedProcess);
			} else {
				// This should mean that znodeStateList is empty
				lastContactedProcess = "";
				verifySystemState();
			}
		}

	}

	private synchronized void createNewProcess() {
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
			logger.error(String.format("Could not create a new process. Error: {}", e));
		}
		logger.info("Created a znode in /state with the dump of the database. New process launched.");
	}

	// *** Getters ***

	public int getPendingProcessesToStart() {
		return pendingProcessesToStart;
	}

	public int getZnodeId() {
		return znodeId;
	}

	public int getLeader() {
		return leader;
	}

	protected int getNodeCreationConfirmed() {
		return nodeCreationConfirmed;
	}

	// *** Sockets related methods ***

	private synchronized void synchronizeWithProcess(String zKStatePath) {
		// Read the data from the znode to get the port
		int port = -1;
		try {
			port = Integer.parseInt(new String(zk.getData(zKStatePath, false, zk.exists(zKStatePath, false)), "UTF-8"));
		} catch (NumberFormatException | UnsupportedEncodingException | KeeperException | InterruptedException e) {
			logger.error(String.format("Could not get the port number. Error is: {}. Exiting...", e));
			System.exit(1);
		}

		try {
			// Send the state via sockets
			if (port != -1) {
				Socket socket = new Socket("localhost", port);
				logger.debug(String.format("Client (leader) should now be connected to the socket, on port {}", port));
				ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
				os.writeObject(DBConn.getDatabase());
				logger.debug("Database sent to the new process.");

				ObjectInputStream is = new ObjectInputStream(socket.getInputStream());
				SocketsOperations response = (SocketsOperations) is.readObject();
				logger.debug(String.format("Follower answer after receiving the database is: {}", response.getResponse()));
				socket.close();

			} else {
				logger.error("Could not get the port number. Error is: {}. Exiting...");
				System.exit(1);
			}
		} catch (Exception e) {
			logger.error(String.format("Error syncing with a the process. Error: {}", e));

		}

	}

	private synchronized void followerStartUp() {

		// Create a port to listen on
		Random rand = new Random();
		int i = rand.nextInt(1000);
		int port = i + 10000; // the port will be in the 10000-11000 range

		boolean dumpCompletedOk = false;

		ServerSocket listener = null;
		
		try {
		// Port to write in /state znode
		byte[] portToSend = String.valueOf(port).getBytes("UTF-8");


		// Create the socket
		listener = new ServerSocket(port);

			// Start listening
			boolean firstTime = true;
			while (true) {
				Socket socket = listener.accept();
				if (firstTime) {
					// Create a znode in /state to notify the leader that the process is ready to
					// synchronize and to notify the listening port
					znodeIdState = zk.create(
							ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT
									+ ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_PREFIX,
							new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
					zk.create(
							ConfigurationParameters.ZOOKEEPER_TREE_STATE_ROOT
									+ ConfigurationParameters.ZOOKEEPER_TREE_STATE_PREFIX,
							portToSend, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
					firstTime = false;
				}

				ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
				ObjectInputStream is = new ObjectInputStream(socket.getInputStream());

				try {
					byte[] dbDump;
					dbDump = (byte[]) is.readObject();
					DBConn.createDatabase(dbDump);
					dumpCompletedOk = true;
					os.writeObject(new SocketsOperations(SocketsOperationEnum.OK));
				} catch (ClassNotFoundException e) {
					os.writeObject(new SocketsOperations(SocketsOperationEnum.ERROR));
					e.printStackTrace();
				} catch (IOException e) {
					os.writeObject(new SocketsOperations(SocketsOperationEnum.ERROR));
					logger.error(String.format("Error syncing the database.", e));
				}

				if (dumpCompletedOk) {
					addToMembers();
				}
			}
		} catch (Exception e) {
			logger.error(String.format("Something happened while syncing with the leader. Killing myself...", e));
		} finally {
			if (dumpCompletedOk == false) {
				logger.debug("Killing myself because I was not able to synchronize with the leader.");
				try {
					listener.close();
				} catch (IOException e) {
					logger.error(String.format("Error closing socket: {}", e));
				}
				System.exit(1);
			}
		}
	}

	// *** Watchers ***

	// Notified when the number of children in the members branch is updated
	private Watcher watcherMember = new Watcher() {
		public void process(WatchedEvent event) {
			try {
				handleZnodesUpdate();
			} catch (KeeperException e) {
				logger.error(String.format("An exception was triggered while handling znodesUpdate. Error: ", e));
			} catch (InterruptedException e) {
				logger.error(String.format("An exception was triggered while handling znodesUpdate. Error: ", e));
			}
		}
	};

	// Notified when the /state directory is modified
	private Watcher watcherState = new Watcher() {
		public void process(WatchedEvent event) {
			handleStateUpdate();
		}
	};
}
