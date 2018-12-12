package es.upm.dit.cnvr.distributedBank.cluster;

import org.apache.log4j.Logger;

import es.upm.dit.cnvr.distributedBank.ConfigurationParameters;

public class Watchdog implements Runnable {
	
	private static Logger logger = Logger.getLogger(Watchdog.class);
	
	private static ClusterManager cm;
	
	public  Watchdog(ClusterManager cm) {
		this.cm = cm;
		logger.debug("Watchdog instantiated.");
	}
	
	@Override
	public void run() {
		logger.debug("Watchdog triggered.");
		while (true) {
			try {
				Thread.sleep(ConfigurationParameters.CLUSTER_WATCHDOG_SLEEP_CYCLE);
			} catch (InterruptedException e) {
				logger.error(String.format("Interrupted exception while sleeping %s", e));
			}
			cm.verifySystemState();
		}
	}

}
