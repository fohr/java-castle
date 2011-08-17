package com.acunu.castle;

/**
 * Callback from async requests
 */
public abstract class Callback extends AbstractCallback
{
	static
	{
		System.load("/usr/lib64/java-castle/libCastleImpl.so");
		init_jni();
	}
	
	private static native void init_jni();

	protected RequestResponse response = null;
	protected int err = 0;

	@Override
	public void process()
	{
		if (response == null)
			throw new IllegalArgumentException("No response was provided to request callback");

		if (err == 0)
			call(response);
		else
			handleError(err);

		/* allow GC of response while we wait for the finalizer */
		response = null; 
	}

	public void setResponse(final RequestResponse response)
	{
		this.response = response;
	}

	public void setErr(final int err)
	{
		this.err = err;
	}

	protected abstract void call(RequestResponse response);

	protected abstract void handleError(int error);
}
