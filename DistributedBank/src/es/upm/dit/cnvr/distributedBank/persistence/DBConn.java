package es.upm.dit.cnvr.distributedBank.persistence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.log4j.Logger;

import es.upm.dit.cnvr.distributedBank.BankClient;

public class DBConn {

	private static Logger logger = Logger.getLogger(DBConn.class);
	
	public static byte[] getDatabase() throws IOException {
		return SerializationUtils.serialize(ClientDBImpl.database);
	}

	public static void createDatabase (byte[] databaseDump) {
		ClientDBImpl.database = SerializationUtils.deserialize(databaseDump);
	}

}
