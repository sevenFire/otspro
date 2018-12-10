package com.baosight.xinsight.ots.client.exception;

import com.baosight.xinsight.ots.exception.OtsException;

public class ConfigException extends OtsException {

	private static final long serialVersionUID = 1L;

	public ConfigException(long errorCode) {
	    super(errorCode);
	}

	public ConfigException(long errorCode, String message) {
		super(errorCode, message);
	}

	public ConfigException(long errorCode, String message, Throwable cause) {
		super(errorCode, message, cause);
	}

	public ConfigException(long errorCode, Throwable cause) {
		super(errorCode, cause);
	}
}