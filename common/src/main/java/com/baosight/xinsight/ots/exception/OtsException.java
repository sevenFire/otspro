package com.baosight.xinsight.ots.exception;

public class OtsException extends Exception {

    private static final long serialVersionUID = 1L;
    private long errorCode;

    public long getErrorCode() {
        return errorCode;
    }

    public OtsException(long errorCode) {
        super();
        this.errorCode = errorCode;
    }

    public OtsException(long errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public OtsException(long errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public OtsException(long errorCode, Throwable cause) {
        super(cause);
        this.errorCode = errorCode;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(ErrorCode:");
        sb.append(errorCode);
        sb.append(")");
        sb.append(super.toString());
        return sb.toString();
    }

    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("(ErrorCode:");
        sb.append(errorCode);
        sb.append(")");
        sb.append(super.getMessage());
        return sb.toString();
    }
}
