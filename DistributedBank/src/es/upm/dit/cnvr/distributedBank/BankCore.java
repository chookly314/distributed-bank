package es.upm.dit.cnvr.distributedBank;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import es.upm.dit.cnvr.distributedBank.cluster.ClusterManager;
import es.upm.dit.cnvr.distributedBank.persistence.*;

public class BankCore {
	
	private static Logger logger = Logger.getLogger(BankCore.class);
	private ZooKeeper zk;
	private boolean leader;
	public boolean updating;
	
	public BankCore () {
		this.leader = true;
		this.updating = false;
		
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
		
		ClusterManager clustermanager = null;
		try {
			clustermanager = new ClusterManager(zk, this);
		} catch (Exception e) {
			logger.error("Can't create Cluster Manager");
		}
		
		//Falta pasarle la base de datos, que no se como vamos a inicializarla. Habrá que modificar esto
		ClientDB database = new ClientDBImpl();
		
		UpdateManager updatemanager = new UpdateManager(this, zk, clustermanager, database);
	}
	
	public static void main(String[] args) {
		
		BankCore bankcore = new BankCore();
		
	}
	
	public boolean isLeader() {
		return this.leader;
	}
	
	public synchronized boolean isUpdating() {
		return updating;
	}
	
	public synchronized void setIsLeader(boolean leader) {
		this.leader = leader;
	}
	
	// *** Watchers ***

	private Watcher sessionWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			logger.info(String.format("ClusterManager Zookeeper session created: {}.", e.toString()));
			notify();
		}
	};
	
	
}
