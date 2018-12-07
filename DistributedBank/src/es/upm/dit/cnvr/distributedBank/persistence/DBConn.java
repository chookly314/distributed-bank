package es.upm.dit.cnvr.distributedBank.persistence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;

import org.apache.log4j.Logger;

import es.upm.dit.cnvr.distributedBank.BankClient;

public class DBConn {

	private static Logger logger = Logger.getLogger(DBConn.class);
	
	public static byte[] getDatabase() throws IOException {
		HashMap<Integer, BankClient> databaseDump = ClientDBImpl.database;
		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(byteOut);
		out.writeObject(databaseDump);
		out.flush();
		return byteOut.toByteArray();
	}

	public static void createDatabase (byte[] databaseDump) {
		try {
			Object obj = null;
			ByteArrayInputStream bis = null;
			ObjectInputStream ois = null;
			bis = new ByteArrayInputStream(databaseDump);
			ois = new ObjectInputStream(bis);
			obj = ois.readObject();
			HashMap<Integer, BankClient> databaseToRestore = (HashMap) obj;
			bis.close();
		} catch (ClassNotFoundException e) {
			logger.error(String.format("Could not cast from object to HashMap. Error: {}", e));
		} catch (IOException e) {
			logger.error(String.format("Could not read input stram. Error: {}", e));
		}
	}

}
