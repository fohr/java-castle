package com.acunu.castle;

import java.io.IOException;

/**
 * An exception from the Castle kernel module. The errno gives more information.
 * To diagnose meaning, try googling errno.h and reading dmesg.
 */
public class CastleException extends IOException
{
	static
	{
		System.load("/usr/lib64/java-castle/libCastleImpl.so");
		init_jni();
	}
	
	static native void init_jni();
	
	private final int errno;
	private final String msg;

	CastleException(int errno, String msg)
	{
		this.errno = errno;
		this.msg = msg;
	}
	
	CastleException(int errno, String msg, Throwable cause)
	{
		super(cause);
		this.errno = errno;
		this.msg = msg;
	}

	public int getErrno()
	{
		return errno;
	}

	@Override
	public String toString()
	{
		return String.format("CastleException(err=%d, msg=\"%s\")", errno, msg);
	}
}