package es.upm.dit.cnvr.distributedBank;

import java.io.Serializable;

public class BankClientImpl implements BankClient, Serializable{

	private int accountNumber;
	private int balance;
	private String name;
	
	public BankClientImpl(int accountNumber, String name, int balance) {
		this.accountNumber = accountNumber;
		this.name=name;
		this.balance=balance;
	}
	
	@Override
	public int getAccount() {
		return accountNumber;
	}

	@Override
	public void setAccount(int accNumber) {
		this.accountNumber = accNumber;
	}

	@Override
	public int getBalance() { 
		return this.balance;
	}

	@Override
	public void setBalance(int balance) {
		this.balance = balance;
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public void setName(String name) {
		this.name=name;
	}
	
	@Override
	public String toString() {

		String string = "";

		string+= "Account: "+Integer.toString(accountNumber)+" ";
		string+= "Name: "+name+" ";
		string+= "Balance: "+Integer.toString(balance);

		return string;
	}
	

}