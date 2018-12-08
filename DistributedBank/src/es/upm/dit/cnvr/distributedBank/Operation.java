package es.upm.dit.cnvr.distributedBank;

import java.io.Serializable;
import es.upm.dit.cnvr.distributedBank.persistence.*;


public class Operation implements Serializable{

	private static final long serialVersionUID = 1L;
	private OperationEnum operation;
	private BankClient client = null;
	private Integer accountNumber = 0;
	private ClientDB clientDB = null;
	private Integer balance = 0;
	
	// ADD_CLIENT, UPDATE_CLIENT
	public Operation(OperationEnum operation, BankClient client) {
		this.operation = operation;
		this.client = client;
	}

	// READ_CLIENT, DELETE_CLIENT
	public Operation(OperationEnum operation, Integer accountNumber) {
		this.operation = operation;
		this.accountNumber = accountNumber;
	}

	public Operation(OperationEnum operation, ClientDB clientDB) {
		this.operation = operation;
		this.clientDB = clientDB;
	}

	public Operation(OperationEnum operation, Integer accountNumber, Integer balance) {
		this.operation = operation;
		this.accountNumber = accountNumber;
		this.balance = balance;
	}
	
	public OperationEnum getOperation() {
		return operation;
	}

	public void setOperation(OperationEnum operation) {
		this.operation = operation;
	}

	public BankClient getClient() {
		return client;
	}

	public void setClient(BankClient client) {
		this.client = client;
	}

	public Integer getAccountNumber() {
		return accountNumber;
	}

	public Integer getBalance() {
		return balance;
	}
	
	public void setAccountNumber(Integer accountNumber) {
		this.accountNumber = accountNumber;
	}
	
	public void setBalance(Integer balance) {
		this.balance = balance;
	}
	
	@Override
	public String toString() {

		String string = null;

		string = "OperationBank [operation=" + operation;
		if (client != null)
			string = string + ", client=" + client.toString();
		string = string + ", accountNumber=" + accountNumber + "]\n";
		if (clientDB != null)
			string = string + clientDB.toString();

		return string;
	}
	
}
