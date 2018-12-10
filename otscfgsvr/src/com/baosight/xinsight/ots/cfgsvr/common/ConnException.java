package com.baosight.xinsight.ots.cfgsvr.common;

public class ConnException extends Exception
{
    public ConnException(String message)
    {
        super(message);
    }

    public ConnException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public ConnException(Throwable cause)
    {
        super(cause);
    }

    public String toString()
    {
        return (new StringBuilder()).append(super.toString()).toString();
    }

    public String getMessage()
    {
        return (new StringBuilder()).append(super.getMessage()).toString();
    }

    private static final long serialVersionUID = 1L;

}
