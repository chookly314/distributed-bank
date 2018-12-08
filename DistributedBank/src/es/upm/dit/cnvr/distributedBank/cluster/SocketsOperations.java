package es.upm.dit.cnvr.distributedBank.cluster;

import java.io.Serializable;

public class SocketsOperations implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private SocketsOperationEnum response;
	
	protected SocketsOperations (SocketsOperationEnum response) {
		this.response = response;
	}
	
	protected SocketsOperationEnum getResponse() {
		return response;
	}
	
}
