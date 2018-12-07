package es.upm.dit.cnvr.distributedBank.persistence;

import java.util.HashMap;
import java.util.Map.Entry;

import es.upm.dit.cnvr.distributedBank.BankClient;
import es.upm.dit.cnvr.distributedBank.ServiceStatusEnum;

public class ClientDBImpl implements ClientDB {

	protected static HashMap<Integer, BankClient> database = new HashMap<Integer, BankClient>();

	public ClientDBImpl () {
	}
	
	@Override
	public ServiceStatusEnum create(BankClient client) {
		if (database.containsKey(client.getAccount())) {
			return ServiceStatusEnum.CLIENT_EXISTED;
		} else {
			database.put(client.getAccount(), client);
			return ServiceStatusEnum.OK;
		}	
	}

	@Override
	public ServiceStatusEnum update(int accountNumber, int balance) {
		if (database.containsKey(accountNumber)) {
			BankClient client = database.get(accountNumber);
			client.setBalance(balance);
			database.put(client.getAccount(), client);
			return ServiceStatusEnum.OK;
		} else {
			return ServiceStatusEnum.NOT_FOUND;
		}
	}

	@Override
	public BankClient read(String clientName) {
		for (Entry <Integer, BankClient>  entry : database.entrySet()) {
			BankClient c = entry.getValue();
			if (c.getName().equals(clientName)) {
				return c;
			}
		}
		return null;
	}

	@Override
	public BankClient read(int accountNumber) {
		if (database.containsKey(accountNumber)) {
			return database.get(accountNumber);
		} else {
			return null;
		}
	}

	@Override
	public ServiceStatusEnum delete(int accountNumber) {
		if (database.containsKey(accountNumber)) {
			database.remove(accountNumber);
			return ServiceStatusEnum.OK;
		} else {
			return ServiceStatusEnum.NOT_FOUND;
		}	
	}

	@Override
	public ServiceStatusEnum delete(String clientName) {
		for (Entry <Integer, BankClient>  entry : database.entrySet()) {
			BankClient c = entry.getValue();
			if (c.getName().equals(clientName)) {
				return ServiceStatusEnum.OK;
			}
		}
		return ServiceStatusEnum.NOT_FOUND;
	}
}
