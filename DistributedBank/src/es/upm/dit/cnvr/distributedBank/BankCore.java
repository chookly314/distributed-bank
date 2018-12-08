package es.upm.dit.cnvr.distributedBank;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import java.util.Scanner;

import es.upm.dit.cnvr.distributedBank.cluster.ClusterManager;
import es.upm.dit.cnvr.distributedBank.persistence.ClientDB;
import es.upm.dit.cnvr.distributedBank.persistence.ClientDBImpl;

public class BankCore {

	//peticion de escritura si este es el lider se pasará el sistema a estado updating y se notificará la operación de escritura al updateManager 
	// si no es el lider contesta diciendo que tiene que contactar con el lider.
	
	private static Logger logger = Logger.getLogger(BankCore.class);
	private ZooKeeper zk;
	private boolean leader;
	public boolean updating;

	public BankCore () {
		this.leader = true;
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
		
		UpdateManager updatemanager = new UpdateManager(this, zk);
		
		
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
					//updatemanager.persistOperation(OperationEnum.CREATE);
					//updateManager.create(client,zk);
					break;
				case 2: // Read client
					if(!updating) {
					System. out .print(">>> Enter account number (int) = ");
					if (sc.hasNextInt()) {
						accNumber = sc.nextInt();
						//leer a partir del número de cuenta
						client = clientDB.read(accNumber);
						System.out.print(client);
				
					} else {
						System.out.println("The text provided is not an integer");
						sc.next();
					}
					}
					else {
						System.out.println("The system is busy, try it again later. Thanks.");
						sc.next();
					}
					break;
				case 3: // Update client
					//Pasar al updateManager
		
					System. out .print(">>> Enter account number (int) = ");
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
					//Pasar al update manager para que actualice
					//updateManager.updateClient(accNumber, balance, zk)
					// o creo clase client y lo paso
					
					break;
				case 4: // Delete client
					System. out .print(">>> Enter account number (int) = ");
					if (sc.hasNextInt()) {
						accNumber = sc.nextInt();
						//pasar a updateManager para que borre
						//updateManager.deleteClient(accNumber);
						//hacer Client y borrar
					} else {
						System.out.println("The text provided is not an integer");
						sc.next();
					}
					break;
				case 5: //listar todos
					if(!updating) {
						//System.out.println(clientDB.read(););		
					}
					break;
				case 6:
					exit = true;	
				default:
					break;
				}
			} catch (Exception e) {
				System.out.println("Exception at Main. Error read data");
			}

		}

		sc.close();
		
	}
	
	public static void main(String[] args) {
		
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
		return this.isUpdating();
		
	}
	
	// *** Watchers ***

	private Watcher sessionWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			logger.info(String.format("ClusterManager Zookeeper session created: {}.", e.toString()));
			notify();
		}
	};
	
}
