package com.baosight.xinsight.ots.client.exception;

import com.baosight.xinsight.ots.exception.OtsException;

public class IndexException extends OtsException {

	private static final long serialVersionUID = 1L;

	public IndexException(long errorCode) {
	    super(errorCode);
	}

	public IndexException(long errorCode, String message) {
		super(errorCode, message);
	}

	public IndexException(long errorCode, String message, Throwable cause) {
		super(errorCode, message, cause);
	}

	public IndexException(long errorCode, Throwable cause) {
		super(errorCode, cause);
	}
}