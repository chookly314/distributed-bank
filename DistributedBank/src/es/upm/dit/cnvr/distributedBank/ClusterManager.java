package es.upm.dit.cnvr.distributedBank;

import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ClusterManager {

	private static Logger logger = Logger.getLogger(ClusterManager.class);

	private List znodesList;
	// Leader sequential znode number in the "members" tree
	private int leader;
	private ZooKeeper zk;

	public ClusterManager() {
		// Create Zookeeper session
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

		if (zk != null) {
			leaderElection();
		}

	}

	// Notified when the session is created
	private Watcher sessionWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			logger.info(String.format("ClusterManager Zookeeper session created: {}.", e.toString()));
			notify();
		}
	};

	private synchronized void leaderElection() {
		try {
			Stat s = zk.exists(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, false);

			// Get current leader
			List<String> list = zk.getChildren(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, false, s);
			groupLeader = getNewLeader(list);
			logger.info(String.format("The current cluster leader is {}", groupLeader));

			// Get zNode to watch (instead of setting a watcher on /members, to avoid herding effect)
			String zNodeToWatch = getNewNodeToWatch(list);
			zk.getChildren(ConfigurationParameters.ZOOKEEPER_TREE_MEMBERS_ROOT, watcherMember, s);

		} catch (

		Exception e) {
			System.out.println("Exception: wacherMember");
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

	// Notified when the number of children in the members branch is updated
	private Watcher watcherMember = new Watcher() {
		public void process(WatchedEvent event) {
			leaderElection();
		}
	};
	
}
