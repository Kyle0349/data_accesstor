package com.qhcs.accesser.exception;

/**
 * 
 * @author liqifei
 */
public class OozieManagerException extends RuntimeException{

	/**
	 * 
	 */
	private static final long serialVersionUID = 3792385237978976165L;
	
    public OozieManagerException() {
		
	}
	
	public OozieManagerException(String msg) {
		super(msg);
	}

}
