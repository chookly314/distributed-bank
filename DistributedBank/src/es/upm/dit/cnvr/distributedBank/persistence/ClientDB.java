package es.upm.dit.cnvr.distributedBank.persistence;

import es.upm.dit.cnvr.distributedBank.BankClient;
import es.upm.dit.cnvr.distributedBank.ServiceStatusEnum;

public interface ClientDB {

	ServiceStatusEnum create(BankClient client);
	
	ServiceStatusEnum update(int accountNumber, int balance);
	
	BankClient read(String clientName);
	
	BankClient read(int accountNumber);
	
	ServiceStatusEnum delete(int accountNumber);
	
	ServiceStatusEnum delete(String clientName);
	
}
