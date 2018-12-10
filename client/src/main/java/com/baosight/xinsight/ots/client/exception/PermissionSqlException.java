package com.baosight.xinsight.ots.client.exception;

import com.baosight.xinsight.ots.exception.OtsException;

public class PermissionSqlException extends OtsException {
	/**
	 * this exception return the exception about permission sql operation
	 */
	private static final long serialVersionUID = 1L;

	public PermissionSqlException(long errorCode) {
		super(errorCode);
	}

	public PermissionSqlException(long errorCode, String message) {
		super(errorCode, message);
	}

	public PermissionSqlException(long errorCode, String message,
			Throwable cause) {
		super(errorCode, message, cause);
	}

	public PermissionSqlException(long errorCode, Throwable cause) {
		super(errorCode, cause);
	}

}
