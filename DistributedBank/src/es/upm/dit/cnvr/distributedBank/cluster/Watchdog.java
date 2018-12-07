package es.upm.dit.cnvr.distributedBank.cluster;

import org.apache.log4j.Logger;

import es.upm.dit.cnvr.distributedBank.ConfigurationParameters;

public class Watchdog implements Runnable {
	
	private static Logger logger = Logger.getLogger(Watchdog.class);
	
	@Override
	public void run() {
		ClusterManager cm = new ClusterManager();
		int pendingProcessesToStart = cm.getPendingProcessesToStart();
		int version = cm.getNodeCreationConfirmed();
		boolean lastTry = false;
		
		while(true) {
			pendingProcessesToStart = cm.getPendingProcessesToStart();
			version = cm.getNodeCreationConfirmed();
			try {
				Thread.sleep(ConfigurationParameters.CLUSTER_WATCHDOG_SLEEP_CYCLE * 1000);
			} catch (InterruptedException e) {
				logger.error(String.format("Watchdog interrupted while sleeping. Error: {}", e));
			}
			int newPendingProcessesToStart = cm.getPendingProcessesToStart();
			int newVersion = cm.getNodeCreationConfirmed();
			if (pendingProcessesToStart < ConfigurationParameters.CLUSTER_GOAL_SIZE) {
				if (version == newVersion) {
					// Process should have been created, but it was not
					// Wait one more time
					if (lastTry == false) {
						lastTry = true;
					} else {
						// Self-destruct the process -> the cluster will be killed if it is not cappable of keeping its server's goal number
						logger.debug(String.format("Watchdog will make the process to stop, because main Thread was not able to create new processes after {} seconds.", ConfigurationParameters.CLUSTER_WATCHDOG_SLEEP_CYCLE * 2 * 1000));
						System.exit(0);
					}
				} else {
					lastTry = true;
				}
			} else {
				lastTry = false;
			}
		}

	}

}
