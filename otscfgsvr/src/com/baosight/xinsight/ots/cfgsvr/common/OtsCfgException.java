package com.baosight.xinsight.ots.cfgsvr.common;

public class OtsCfgException extends Exception{
private static final long serialVersionUID = 1L;
	
	private long errcode;
	
	public long getErrcode() {
        return errcode;
    }

    public OtsCfgException(long errcode) {
        this.errcode = errcode;
    }

    public OtsCfgException(long errcode, String message) {
        super(message);
        this.errcode = errcode;
    }

    public OtsCfgException(long errcode, String message, Throwable cause) {
        super(message, cause);
        this.errcode = errcode;
    }

    public OtsCfgException(long errcode, Throwable cause) {
        super(cause);
        this.errcode = errcode;
    }

    public String toString() {
        return (new StringBuilder()).append("(ErrorCode:").append(errcode).append(")").append(super.toString()).toString();
    }

    public String getMessage() {
        return (new StringBuilder()).append(super.getMessage()).toString();
    }

}
