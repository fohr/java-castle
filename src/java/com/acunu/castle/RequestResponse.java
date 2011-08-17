package com.acunu.castle;

/**
 * The response from Castle to a given request.
 */
public class RequestResponse
{
	static
	{
		System.load("/usr/lib64/java-castle/libCastleImpl.so");
		init_jni();
	}
	
	static native void init_jni();
	
	public final boolean found;
	public final long length;
	public final long token;

	public RequestResponse(boolean found, long length, long token)
	{
		this.found = found;
		this.length = length;
		this.token = token;
	}
}
