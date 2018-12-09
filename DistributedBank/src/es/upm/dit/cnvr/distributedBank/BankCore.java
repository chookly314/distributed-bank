package es.upm.dit.cnvr.distributedBank;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import java.util.Scanner;

import es.upm.dit.cnvr.distributedBank.cluster.ClusterManager;
import es.upm.dit.cnvr.distributedBank.persistence.*;


public class BankCore {

	//peticion de escritura si este es el lider se pasará el sistema a estado updating y se notificará la operación de escritura al updateManager 
	// si no es el lider contesta diciendo que tiene que contactar con el lider.
	
	private static Logger logger = Logger.getLogger(BankCore.class);
	private ZooKeeper zk;
	private boolean leader;
	public boolean updating;

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
				zk = new ZooKeeper(ZookeeperServersEnum.getRandomServer(),
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
					client = readClient(sc);
					//Pasar a updateManager para crear este cliente.
					updatemanager.processOperation(new Operation(OperationEnum.CREATE, client));
					logger.info("End of creation, breaking case.");
					break;
				case 2: // Read client
					
					System. out .print(">>> Enter account number (int) = ");
					if (sc.hasNextInt()) {
						accNumber = sc.nextInt();
						//leer a partir del número de cuenta
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
					/*else {
						System.out.println("The system is busy, try it again later. Thanks.");
						sc.next();
					}*/
					break;
				case 3: // Update client
					//Pasar al updateManager
		
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
				default:
					break;
				}
			} catch (Exception e) {
				logger.error("Error reading data: "+e.toString());
			}

		}

		sc.close();
		
	}
	
	public static void main(String[] args) {
        String hostOS = System.getProperty("os.name");
        // Guess the OS
        // + 0: macOS
        // +1: Linux
        int os = -1;
        if (hostOS.contains("Mac")) {
        	os = 0;
        } else {
        	//TODO refine this to be sure that we are on Linux here
        	os = 1;
        }
		
		BasicConfigurator.configure();
		// Used when new processes need to be created
		if (args.length != 1) {
			logger.error("You should pass your working directory as paramerer. Example: java -cp ... app.jar /home/user. Exiting...");
			System.exit(1);
		} else {
			if (os == 0) {
				ConfigurationParameters.PROJECT_WORKING_DIRECTORY = args[0] + "/";
				ConfigurationParameters.SERVER_CREATION = ConfigurationParameters.SERVER_CREATION_PREFIX_MAC + ConfigurationParameters.PROJECT_WORKING_DIRECTORY + ConfigurationParameters.PROJECT_START_SCRIPT + ConfigurationParameters.SERVER_CREATION_SUFIX_MAC;
				logger.info("Host OS is macOS.");
			} if (os == 1) {
				ConfigurationParameters.PROJECT_WORKING_DIRECTORY = args[0] + "/";
				ConfigurationParameters.SERVER_CREATION = ConfigurationParameters.SERVER_CREATION_PREFIX_LINUX + ConfigurationParameters.PROJECT_WORKING_DIRECTORY + ConfigurationParameters.PROJECT_START_SCRIPT + ConfigurationParameters.SERVER_CREATION_SUFIX_LINUX;
				logger.info("Host OS is Linux.");
			}
			logger.info(String.format("Working directory is %s", ConfigurationParameters.PROJECT_WORKING_DIRECTORY));
			logger.info(String.format("Server creation command will be: %s", ConfigurationParameters.SERVER_CREATION));			
		}
		BankCore bankcore = new BankCore();
	}
	
	
	public BankClient readClient(Scanner sc) {
		int accNumber = 0;
		String name   = null;
		int balance   = 0;
		
		System. out .print(">>> Enter account number (int) = ");
		if (sc.hasNextInt()) {
			accNumber = sc.nextInt();
		} else {
			System.out.println("The provised text provided is not an integer");
			sc.next();
			return null;
		}

		System. out .print(">>> Enter name (String) = ");
		name = sc.next();

		System. out .print(">>> Enter balance (int) = ");
		if (sc.hasNextInt()) {
			balance = sc.nextInt();
		} else {
			System.out.println("The provised text provided is not an integer");
			sc.next();
			return null;
		}
		return new BankClientImpl(accNumber, name, balance);
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

