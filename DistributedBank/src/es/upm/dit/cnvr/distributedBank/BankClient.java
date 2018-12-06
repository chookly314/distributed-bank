package es.upm.dit.cnvr.distributedBank;

public interface BankClient {
	
	int getAccount();
	
	void setAccount(int accNumber);
	
	int getBalance();
	
	void setBalance(int balance);
	
	String getName();
	
	void setName(String name);
	
	String toString();
}
