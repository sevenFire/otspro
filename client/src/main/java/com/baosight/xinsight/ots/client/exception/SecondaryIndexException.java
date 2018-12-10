package com.baosight.xinsight.ots.client.exception;

import com.baosight.xinsight.ots.exception.OtsException;

public class SecondaryIndexException extends OtsException {
	private static final long serialVersionUID = 1L;

	public SecondaryIndexException(long errorCode) {
	    super(errorCode);
	}

	public SecondaryIndexException(long errorCode, String message) {
		super(errorCode, message);
	}

	public SecondaryIndexException(long errorCode, String message, Throwable cause) {
		super(errorCode, message, cause);
	}

	public SecondaryIndexException(long errorCode, Throwable cause) {
		super(errorCode, cause);
	}
}
