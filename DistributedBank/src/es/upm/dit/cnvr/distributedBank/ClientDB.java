package es.upm.dit.cnvr.distributedBank;

public interface ClientDB {

	ServiceStatusEnum create(BankClient client);
	
	ServiceStatusEnum update(int accountNumber, int balance);
	
	BankClient read(String clientName);
	
	BankClient read(int accountNumber);
	
	ServiceStatusEnum delete(int accountNumber);
	
	ServiceStatusEnum delete(String clientName);
	
}
