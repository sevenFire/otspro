package com.baosight.xinsight.ots.client.exception;

import com.baosight.xinsight.ots.exception.OtsException;

public class TableException extends OtsException {

	private static final long serialVersionUID = 1L;

	public TableException(long errorCode) {
	    super(errorCode);
	}

	public TableException(long errorCode, String message) {
		super(errorCode, message);
	}

	public TableException(long errorCode, String message, Throwable cause) {
		super(errorCode, message, cause);
	}

	public TableException(long errorCode, Throwable cause) {
		super(errorCode, cause);
	}
}