package es.upm.dit.cnvr.distributedBank.cluster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
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
	private static Integer znodeId;
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
	private static Watchdog watchdog = null;
	// This variable will prevent the system from counting the same znode creation
	// twice
	// BankCore instance used to notify when the system is restoring
	private BankCore bankCore;
	// List of servers killed locks to remove once the system fully restored. Full
	// paths
	private static ArrayList<Integer> pendingLocksToRemove = new ArrayList<Integer>();
	// Will be true if a new leader is elected during an operation. This will
	// trigger a ops followed by locks znodes deletion
	boolean newLeaderElectedDuringUpdate = false;
	// /state znode id of the last contacted process, this variable is only used by
	// the leader
	private static String lastContactedProcess = "";
	private boolean znodeDownDuringUpdate = false;

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
				logger.info(String.format("Killing the process because membersCreation is %s and stateCreation is %s",
						membersCreation, stateCreation));
				System.exit(1);
			}

			// Check if there are any members
			znodeListMembers = getZnodeList(false);
			// System.out.println(znodeListMembers.toString());
			if (znodeListMembers.isEmpty()) {
				// This process is the first one, the first leader
				addToMembers();
				znodeListMembers = getZnodeList(true);
				leaderElection();
//				verifySystemState();
			} else {
				// We get the list again just in case something weird happened
				if (zk.getChildren(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, false, zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, false)).size() >= ConfigurationParameters.CLUSTER_GOAL_SIZE) {
					logger.debug("The cluster already has its goal servers number. Killing myself.");
					System.exit(1);
				} else {
					// Create the socket and add a node to /state to tell the leader to contact me
					// to synchronize the state
					logger.info("New process started. Going to create a socket...");
					followerStartUp();
				}
			}
		}
	}

	private boolean createZookeeperDirectory(String dir) {
		try {
			String response = new String();
			Stat s = zk.exists(dir, false);
			if (s == null) {
				response = zk.create(dir, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				logger.info(String.format("%s directory created. Response: %s", dir, response));
			}
			return true;
		} catch (KeeperException | InterruptedException e) {
			logger.error(String.format("Could not create Zookeeper %s directory. Error: ", dir, e));
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
			logger.debug(String.format("Created zNode member id: %s", znodeIDString));
			znodeId = Integer.valueOf(znodeIDString);
			// Set a watcher - no need: this method is always called after getZnodeList
//			zk.getChildren(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, watcherMember);				
		} catch (KeeperException e) {
			logger.error(String.format("Error creating a znode in members. Exiting to avoid inconsistencies: %s",
					e.toString()));
			System.exit(1);
		} catch (InterruptedException e) {
			logger.error(String.format("Error creating a znode in members. Exiting to avoid inconsistencies: %s",
					e.toString()));
			System.exit(1);
		} catch (NumberFormatException e) {
			logger.error(e.toString());
		}
	}

	public synchronized ArrayList<Integer> getZnodeList(boolean setWatcher) {
		Stat s = null;
		try {
			s = zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, false);
		} catch (KeeperException | InterruptedException e) {
			logger.error(String.format("Error getting the members tree: %s", e.toString()));
		}
		logger.debug("Calling getZnodeList with watcher " + setWatcher);
		if (s != null) {
			List<String> znodeListString = null;
			try {
				if (setWatcher == true) {
					znodeListString = zk.getChildren(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, watcherMember, s);
					znodeListMembersString = znodeListString;					
				} else {
					znodeListString = zk.getChildren(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, false, s);
					znodeListMembersString = znodeListString;
				}
			} catch (KeeperException e) {
				logger.error(String.format("Error getting members znodes tree. Exiting to avoid inconsistencies: %s",
						e.toString()));
				System.exit(1);
			} catch (InterruptedException e) {
				logger.error(String.format("Error getting members znodes tree. Exiting to avoid inconsistencies: %s",
						e.toString()));
				System.exit(1);
			}
			if (znodeListString == null) {
				return null;
			}
			// Parse and convert the list to int
			ArrayList<Integer> newZnodeList = new ArrayList<Integer>();
			for (String znode : znodeListString) {
				// logger.info(znode);
				znode = znode.replace(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_PREFIX_NO_SLASH, "");
				// logger.info(znode);
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
			logger.error(String.format("Error getting the state tree: %s", e.toString()));
		}

		if (s != null) {
			try {
				return zk.getChildren(ConfigurationParameters.ZOOKEEPER_TREE_STATE_ROOT, watcherState, s);
			} catch (KeeperException e) {
				logger.error(String.format("Error getting members znodes tree. Exiting to avoid inconsistencies: %s",
						e.toString()));
				System.exit(1);
			} catch (InterruptedException e) {
				logger.error(String.format("Error getting members znodes tree. Exiting to avoid inconsistencies: %s",
						e.toString()));
				System.exit(1);
			}
		}
		return null;
	}

	// Only accessed by the leader
	protected synchronized void verifySystemState() {
//		logger.info("Enter verifySystemState");
		pendingProcessesToStart = ConfigurationParameters.CLUSTER_GOAL_SIZE - znodeListMembers.size();
//		logger.debug(String.format("Pending processes to start number is %d", pendingProcessesToStart));
		if (pendingProcessesToStart > 0) {
//			logger.debug("Going to set up a new server");
			setUpNewServer();
		} else if (pendingProcessesToStart == 0) {
			if (znodeDownDuringUpdate == true && (leader == znodeId)) {
				if (newLeaderElectedDuringUpdate) {
					undoCurrentOperation();
					newLeaderElectedDuringUpdate = false;				
				} else {
					// Get /locks znodes and delete them
//					try {
						
						
//						 Final state for the restoring process
//						 Remove pending processes, it there are any
						for (Integer process : pendingLocksToRemove) {
							logger.debug(String.format("Removing process %s from /locks", process.toString()));
							logger.debug(String.format("What I am actually trying to remove is %s", ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT + ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_PREFIX + 
										 process));
							try {
								zk.delete(ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT + ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_PREFIX  + String.valueOf(process), -1);
							} catch (InterruptedException e) {
								logger.error(String.format("Problem removing pending lock %s. Error: %s", process, e));
							} catch (KeeperException e) {
								logger.error(String.format("Problem removing pending lock %s. Error: %s", process, e));
							}
						}
//						 Reset pending processes
						logger.debug("Reseting pending locks to remove.");
						pendingLocksToRemove = new ArrayList<Integer>();
						
						
						
//						List<String> locks = getLocks();
//						if (locks.size() > 0) {
//							for (String znode : locks) {
//								if (!(znode.equals(ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_PREFIX_NO_SLASH + znodeId.toString()))) {
//									logger.debug(String.format("Removing znode %s from locks, which should be %s", znode, ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT + "/" + znode));
//									zk.delete(ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT + "/" + znode, zk.exists(znode, false).getVersion());									
//								}
//							}
//						}
//					} catch (KeeperException e) {
//						logger.error(String.format("Could not get the list of znodes in /locks. Error: %s", e));
//					} catch (InterruptedException e) {
//						logger.error(String.format("Could not get the list of znodes in /locks. Error: %s", e));
//					}
				}
				znodeDownDuringUpdate = false;
			}	
		} else {
			logger.error("There are more processes than expected.");
		}
	}

	private synchronized void leaderElection() {
		try {
			logger.debug("Electing new leader.");
			Stat s = zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, false);
			
			// Get current leader
			leader = getLowestId(znodeListMembers);
			logger.debug(String.format("Znodelist is %s", znodeListMembers.toString()));
			logger.debug(String.format("Leader should be %d, and I am %d", leader, znodeId));
			if (znodeId == leader) {
				logger.info("I am the new leader!");
				bankCore.setIsLeader(true);
				// Create the watchdog thread 
				watchdog = new Watchdog(this);
				Thread t = new Thread(watchdog);
				t.start();
				logger.debug("New thread with watchdog created.");
				// A leader won't check again if he is the leader, so we set a /state watcher
				// here and update the znodes list
				znodeListState = getZnodeStateList();
			} else {
				bankCore.setIsLeader(false);
			}
		} catch (Exception e) {
			logger.error(String.format("Error electing leader: %s", e.toString()));
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
		String command = ConfigurationParameters.SERVER_CREATION;
		StringBuffer output = new StringBuffer();
		Process p;
		try {
			logger.debug(String.format("Executing command %s to create a new process.", command));
			p = Runtime.getRuntime().exec(command);
//			p.waitFor();
			logger.debug("New terminal should pop up.");
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = "";
			while ((line = reader.readLine()) != null) {
				output.append(line + "\n");
			}
		} catch (Exception e) {
			logger.error(String.format("Could not create a new process. Error: %s", e));
		}
		logger.info("New process launched triggered.");
	}

	private synchronized void handleZnodesUpdate() throws KeeperException, InterruptedException {
		// Get the most recent znodeList (by asking Zookeeper) and set a watcher to
		// avoid the possibility of missing information
//		ArrayList<String> oldZnodeListString = new ArrayList<>(znodeListMembersString);
		ArrayList<Integer> oldZnodeList = new ArrayList<>(znodeListMembers);
		logger.info(String.format("IN handle znodes update, oldZnodeListString before is: %s", oldZnodeList.toString()));
		ArrayList<Integer> updatedZnodeList = getZnodeList(true);
		// Check if the event was a znode creation
		if (updatedZnodeList == null) {
			logger.error("Error getting current znode list while handling an update.");
			throw new NullPointerException(
					"znodeList is null and that should not be possible if things work as expected.");
		}

		logger.info(String.format("IN handle znodes update, oldZnodeListString after is: %s", oldZnodeList.toString()));
		
		// This variable will help us to determine when is the update process completed
		// by this process
		boolean updateHandlePending = true;
		while (updateHandlePending) {
			// Check if this process is the leader
			boolean isLeader = (znodeId == leader) ? true : false;
			logger.info(String.format("oldZnodeListString is %s", oldZnodeList.toString()));
			logger.info(String.format("znodeListMembersString is %s", znodeListMembersString.toString()));
			logger.debug("Is leader is " + isLeader);
			if (isLeader) {
				updateHandlePending = false;
				for (Integer id : oldZnodeList) {
					logger.info("iterating to detect possible removed nodes ");
					if (!(znodeListMembersString.contains(id))) {
						logger.info("BankCore updating: " + bankCore.isUpdating());
						if (bankCore.isUpdating()) {
							logger.debug("Znode failed while system updating");
							znodeDownDuringUpdate = true;
							pendingLocksToRemove.add(id);
							logger.info(String.format("znode %s has been removed", id.toString()));
						}
					}
				}
				znodeListMembers = updatedZnodeList;
//				verifySystemState();
			} else {
				znodeListMembers = updatedZnodeList;
				leaderElection();
				updateHandlePending = false;
				if (znodeId == leader) {
					updateHandlePending = true;
					if (bankCore.isUpdating()) {
						znodeDownDuringUpdate = true;
						newLeaderElectedDuringUpdate = true;
					}
				}
			}
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
			logger.error(String.format("Could not get the list of znodes in /operations. Error: %s", e));
		} catch (InterruptedException e) {
			logger.error(String.format("Could not get the list of znodes in /operations. Error: %s", e));
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
			logger.error(String.format("Could not get the list of znodes in /locks. Error: %s", e));
		} catch (InterruptedException e) {
			logger.error(String.format("Could not get the list of znodes in /locks. Error: %s", e));
		}
	}

	private synchronized List<String> getLocks() throws KeeperException, InterruptedException {
		return zk.getChildren(ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT, false,
				zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_LOCKS_ROOT, false));
	}

	// Only accessed by the leader
	private synchronized void handleStateUpdate() {
		logger.debug("Handling znodeStateUpdate...");
		znodeListState = getZnodeStateList();
		// This method should be called only by the leader, but verify it just in case
		if (leader != znodeId) {
			return;
		}


		if (lastContactedProcess.equals("")) {
			if (znodeListState.size() != 0) {
				lastContactedProcess = getLowestStateIdZnode(znodeListState);
				logger.debug(String.format("Contacting with process %s", lastContactedProcess));
				synchronizeWithProcess(lastContactedProcess);
			}
		} else {
			if ((znodeListState.size() != 0) && (getLowestStateIdZnode(znodeListState).equals(lastContactedProcess))) {
				return;
			} else if ((znodeListState.size() != 0)
					&& !(getLowestStateIdZnode(znodeListState).equals(lastContactedProcess))) {
				lastContactedProcess = getLowestStateIdZnode(znodeListState);
				synchronizeWithProcess(lastContactedProcess);
			} else {
				// This should mean that znodeStateList is empty
				lastContactedProcess = "";
//				verifySystemState();
			}
		}

	}

	private synchronized String getLowestStateIdZnode(List<String> znodes) {
		String lowest = znodes.get(0);
		for (String znode : znodes) {
			if (znode.compareTo(lowest) < 0) {
				lowest = znode;
			}
		}
		return lowest;
	}


	// *** Getters ***

	public synchronized int getPendingProcessesToStart() {
		return pendingProcessesToStart;
	}

	public synchronized int getZnodeId() {
		return znodeId;
	}

	public synchronized int getLeader() {
		return leader;
	}

	protected synchronized int getNodeCreationConfirmed() {
		return nodeCreationConfirmed;
	}

	// *** Sockets related methods ***

	private synchronized void synchronizeWithProcess(String zKStatePath) {
		// Read the data from the znode to get the port
		int port = -1;
		try {
			logger.info(String.format("Getting port from the process with /state znode %s.",
					ConfigurationParameters.ZOOKEEPER_TREE_STATE_ROOT + "/" + zKStatePath));
			port = Integer.parseInt(new String(
					zk.getData(ConfigurationParameters.ZOOKEEPER_TREE_STATE_ROOT + "/" + zKStatePath, false,
							zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_STATE_ROOT + "/" + zKStatePath, false)),
					"UTF-8"));
			logger.info(String.format("Port: %d", port));
		} catch (NumberFormatException | UnsupportedEncodingException | KeeperException | InterruptedException e) {
			logger.error(String.format("Could not get the port number. Error is: %s. Exiting...", e));
			System.exit(1);
		}

		try {
			// Send the state via sockets
			if (port != -1) {
				Socket socket = new Socket("localhost", port);
				logger.debug(String.format("Client (leader) should now be connected to the socket, on port %s", port));
				ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
				os.writeObject(DBConn.getDatabase());
				logger.debug("Database sent to the new process.");

				ObjectInputStream is = new ObjectInputStream(socket.getInputStream());
				SocketsOperations response = (SocketsOperations) is.readObject();
				logger.debug(
						String.format("Follower answer after receiving the database is: %s", response.getResponse()));
				socket.close();

			} else {
				logger.error("Could not get the port number. Error is: %s. Exiting...");
				System.exit(1);
			}
		} catch (Exception e) {
			logger.error(String.format("Error syncing with a the process. Error: %s", e));

		}

	}

	private synchronized void followerStartUp() {

		// Create a port to listen on
		Random rand = new Random();
		int i = rand.nextInt(1000);
		int port = i + 10000; // the port will be in the 10000-11000 range

		logger.debug(String.format("The process will listen in the port %d", port));

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
				// TODO Consider making it non-blocking and create the socket before the znode
				// https://www.developer.com/java/data/what-is-non-blocking-socket-programming-in-java.html
				if (firstTime) {
					// Create a znode in /state to notify the leader that the process is ready to
					// synchronize and to notify the listening port
					logger.debug("Creating a znode in /state");
					znodeIdState = zk.create(
							ConfigurationParameters.ZOOKEEPER_TREE_STATE_ROOT
									+ ConfigurationParameters.ZOOKEEPER_TREE_STATE_PREFIX,
							portToSend, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

					firstTime = false;
				}

				Socket socket = listener.accept();
				ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
				ObjectInputStream is = new ObjectInputStream(socket.getInputStream());

				logger.debug("Data received");

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
					znodeListMembers = getZnodeList(true);
					addToMembers();
					zk.delete(znodeIdState, -1);
					logger.info("znode from state deleted");
					break;
				}
			}
		} catch (Exception e) {
			logger.error(String.format("Something happened while syncing with the leader. Killing myself...", e));
			System.exit(1);
		} finally {
			if (dumpCompletedOk == false) {
				logger.debug("Killing myself because I was not able to synchronize with the leader.");
				try {
					listener.close();
				} catch (IOException e) {
					logger.error(String.format("Error closing socket: %s", e));
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
