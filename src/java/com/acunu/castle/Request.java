package com.acunu.castle;

import java.nio.ByteBuffer;

/**
 * The abstract superclass of all requests (i.e. operations) that may be sent to
 * Castle.
 */
abstract class Request
{
	static
	{
		System.load("/usr/lib64/java-castle/libCastleImpl.so");
		init_jni();
	}
	
	private static native void init_jni();
	public static native long alloc(int num);
	public static native void free(long reqs);

	protected static int copyKey(Key key, ByteBuffer buffer)
	{
		try
		{
			return key.copyToBuffer(buffer);
		} catch (CastleException e)
		{
			throw new RuntimeException(e);
		}
	}

    abstract void copy_to(long buffer, int index) throws CastleException;
}
