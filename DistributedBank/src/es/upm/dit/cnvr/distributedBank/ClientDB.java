package es.upm.dit.cnvr.distributedBank;

public interface ClientDB {

	ServiceStatusEnum create(BankClient client);
	
	ServiceStatusEnum update(BankClient client);
	
	BankClient read(String clientName);
	
	BankClient read(int accNumber);
	
	ServiceStatusEnum delete(int accNumber);
	
	ServiceStatusEnum delete(String name);
	
}
