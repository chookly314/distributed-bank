package es.upm.dit.cnvr.distributedBank;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Scanner;



import es.upm.dit.cnvr.distributedBank.cluster.ClusterManager;
import es.upm.dit.cnvr.distributedBank.persistence.*;


public class BankCore {

	private static Logger logger = Logger.getLogger(BankCore.class);
	private ZooKeeper zk;
	private boolean leader;
	public boolean updating;
	public static String zookeeperIpAddress;

	public BankCore () {
		this.leader = false;
		this.updating = false;		
		Scanner sc = new Scanner(System.in);
		boolean exit    = false;
		boolean correct = false;
		int menuKey = 0;
		int accNumber= 0;
		int balance     = 0;
		BankClient client   = null;
		ClientDB clientDB = new ClientDBImpl();
	
		try {
			if (zk == null) {
				String zkServer = ZookeeperServersEnum.getRandomServer();
				logger.info("Connecting to: "+zkServer);
				zk = new ZooKeeper(zkServer,
						ConfigurationParameters.ZOOKEEPER_SESSION_TIMEOUT, sessionWatcher);
				logger.info("Created ZK session");
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
			while (ClusterManager.isInitializing) {
					Thread.sleep(50);
			}
		} catch (Exception e) {
			logger.error(String.format("Can't create Cluster Manager: %s", e.toString()));
		}
		UpdateManager updatemanager = new UpdateManager(this, zk, clustermanager, clientDB);
				
		
		while (!exit) {
			try {
				correct = false;
				menuKey = 0;
				while (!correct) {
					System. out .println(">>> Enter opn cliente.: 1) Create. 2) Read. 3) Update. 4) Delete. 5) BankDB. 6) Exit");				
					if (sc.hasNextInt()) {
						menuKey = sc.nextInt();
						correct = true;
					} else {
						sc.next();
						System.out.println("The text provided is not an integer");
					}
				}

				switch (menuKey) {
				case 1: // Create client
					if (ClusterManager.pendingProcessesToStart != 0) {
						System.out.println("The system is being restored, 'create' operations are disabled for the moment. Please, wait");
					}
					client = readClient(sc);
					if(client!=null) {
					updatemanager.processOperation(new Operation(OperationEnum.CREATE, client));
					logger.info("End of creation, breaking case.");
					}
					break;
				case 2: // Read client
					
					System. out .print(">>> Enter account number (int) = ");
					if (sc.hasNextInt()) {
						accNumber = sc.nextInt();
						client = clientDB.read(accNumber);
						if (client != null) {
							System.out.println(client.toString());
						} else {
							System.out.println("Sorry, the requested account doesn't exist");
						}
				
					} else {
						System.out.println("The text provided is not an integer");
						sc.next();
					}
					break;
				case 3: // Update client
					if (ClusterManager.pendingProcessesToStart != 0) {
						System.out.println("The system is being restored, 'update' operations are disabled for the moment. Please, wait");
					}
					System.out.print(">>> Enter account number (int) = ");
					if (sc.hasNextInt()) {
						accNumber = sc.nextInt();
					} else {
						System.out.println("The text provided is not an integer");
						sc.next();
					}
					System. out .print(">>> Enter balance (int) = ");
					if (sc.hasNextInt()) {
						balance = sc.nextInt();
					} else {
						System.out.println("The text provided is not an integer");
						sc.next();
					}
					updatemanager.processOperation(new Operation(OperationEnum.UPDATE, accNumber, balance));
					break;
				case 4: // Delete client
					if (ClusterManager.pendingProcessesToStart != 0) {
						System.out.println("The system is being restored, 'delete' operations are disabled for the moment. Please, wait");
					}
					System. out .print(">>> Enter account number (int) = ");
					if (sc.hasNextInt()) {
						accNumber = sc.nextInt();
						updatemanager.processOperation(new Operation(OperationEnum.DELETE, accNumber));
					} else {
						System.out.println("The text provided is not an integer");
						sc.next();
					}
					break;
				case 5: // List all
					for (BankClient user : clientDB.readAll()) {
						System.out.println(user.toString());
					}
					break;
				case 6:
					exit = true;
					// Since the leader also has the watchdog thread, we need to kill the process instead
					if (leader) {
						System.exit(1);
					}
				default:
					break;
				}
			} catch (Exception e) {
				logger.error("Error reading data: "+e.toString());
			}
			updating = false;
		}

		sc.close();
		
	}
	
	public static void main(String[] args) {	
       
		int os = getOs();
		
		// Used when new processes need to be created
		if (args.length != 2) {
			logger.error("You should pass your working directory as paramerer. Example: java -cp ... app.jar /home/user. Exiting...");
			logger.error("The second parameter must be a Zookeeper IP address (assumes all nodes collocated)");
			System.exit(1);
		} else {
			
			zookeeperIpAddress = args[1];
			
			String ip = null;
			if (os == 0) {
				logger.info("Host OS is macOS.");
				ConfigurationParameters.PROJECT_WORKING_DIRECTORY = args[0] + "/";
				ConfigurationParameters.SERVER_CREATION_MAC[3] = ConfigurationParameters.PROJECT_WORKING_DIRECTORY 
						+ ConfigurationParameters.PROJECT_START_SCRIPT + " " + zookeeperIpAddress;
				logger.info(String.format("Server creation command will be: %s", Arrays.toString(ConfigurationParameters.SERVER_CREATION_MAC)));			
				ip = getIP(ConfigurationParameters.MACOS_NETWORK_INTERFACE_NAME); 
			} if (os == 1) {
				logger.info("Host OS is Linux.");
				ConfigurationParameters.PROJECT_WORKING_DIRECTORY = args[0] + "/";
				ConfigurationParameters.SERVER_CREATION_LINUX[4] = ConfigurationParameters.PROJECT_WORKING_DIRECTORY 
						+ ConfigurationParameters.PROJECT_START_SCRIPT + " " + zookeeperIpAddress;
				logger.info(String.format("Server creation command will be: %s", Arrays.toString(ConfigurationParameters.SERVER_CREATION_LINUX)));			
				ip = getIP(ConfigurationParameters.LINUX_NETWORK_INTERFACE_NAME_1);
				logger.info("IP not me is " + ip);
				if (ip==null) {
					ip = getIP(ConfigurationParameters.LINUX_NETWORK_INTERFACE_NAME_2);					
				}
			}
			
			if ( ip != null && ip.substring(0, 1).equals("/")) {
				ip = ip.substring(1, ip.length());
			}
			ConfigurationParameters.HOST_IP_ADDRESS = ip;
			if (ConfigurationParameters.HOST_IP_ADDRESS == null) {
				logger.error("Host detected IP address is null. That means there was a problem while trying to find it. Exiting.");
				System.exit(1);
			}
			logger.info(String.format("Your detected IP address is: %s", ConfigurationParameters.HOST_IP_ADDRESS));
			logger.info(String.format("Working directory is %s", ConfigurationParameters.PROJECT_WORKING_DIRECTORY));
			
		}
		BankCore bankcore = new BankCore();
	}
	
	public static int getOs() {
		 String hostOS = System.getProperty("os.name");
	        // Guess the OS
	        // + 0: macOS
	        // +1: Linux
	        int os = -1;
	        if (hostOS.contains("Mac")) {
	        	return 0;
	        } else {
	        	//Note: Would be nice to refine this to be sure that we are on Linux here
	        	return 1;
	        }
	}
	
	
	public BankClient readClient(Scanner sc) {
		int accNumber = 0;
		String name   = null;
		int balance   = 0;
		
		System. out .print(">>> Enter account number (int) = ");
		if (sc.hasNextInt()) {
			accNumber = sc.nextInt();
		} else {
			System.out.println("The text provided is not an integer");
			sc.next();
			return null;
		}

		System. out .print(">>> Enter name (String) = ");
		name = sc.next();

		System. out .print(">>> Enter balance (int) = ");
		if (sc.hasNextInt()) {
			balance = sc.nextInt();
		} else {
			System.out.println("The text provided is not an integer");
			sc.next();
			return null;
		}
		return new BankClientImpl(accNumber, name, balance);
	}

	private static String getIP(String interfaceName) {
		logger.info("Getting IP");
		NetworkInterface networkInterface;
		try {
			networkInterface = NetworkInterface.getByName(interfaceName);
			Enumeration<InetAddress> inetAddress = networkInterface.getInetAddresses();
			InetAddress currentAddress;
			currentAddress = inetAddress.nextElement();
			while(inetAddress.hasMoreElements())
			{
				currentAddress = inetAddress.nextElement();
				if(currentAddress instanceof Inet4Address && !currentAddress.isLoopbackAddress())
				{
					logger.info("IP returned is " + currentAddress.toString());
					return currentAddress.toString();
				}
			}
		} catch (SocketException e) {
			logger.error(String.format("Error finding host IP. Error: %s", e));
		} catch (NullPointerException e) {
			logger.error(String.format("Error getting host IP, probably because the network interface wasn't found. Error: %s", e));
		}
		logger.info("IP returned is " + null);
		return null;
	}
	
	public boolean isLeader() {
		return this.leader;
	}
	
	public boolean isUpdating() {
		return this.updating;
	}
	
	public synchronized void setIsLeader(boolean leader) {
		this.leader = leader;
		logger.info("Setting a leader");
	}
	
	// *** Watchers ***

	private Watcher sessionWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			logger.info(String.format("ClusterManager Zookeeper session created: %s.", e.toString()));
			notify();
		}
	};
}

