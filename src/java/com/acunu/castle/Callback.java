package com.acunu.castle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Callback from async requests
 */
public abstract class Callback implements Runnable
{
	protected final Map<BufferManager, List<ByteBuffer>> buffers = new HashMap<BufferManager, List<ByteBuffer>>();
	protected RequestResponse response = null;
	protected int err = 0;

	@Override
	public void run()
	{
		if (response == null)
			throw new IllegalArgumentException("No response was provided to request callback");

		try
		{
			if (err == 0)
				call(response);
			else
				handleError(err);
		} finally
		{
			cleanup();
		}
		
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

	public void collect(final BufferManager manager, final ByteBuffer buffer)
	{
		if (!buffers.containsKey(manager))
			buffers.put(manager, new LinkedList<ByteBuffer>());
		buffers.get(manager).add(buffer);
	}

	/*
	 * last chance to avoid leaking NB: finalize methods are not guaranteed to
	 * be run, ever.
	 */
	@Override
	public void finalize()
	{
		cleanup();
	}

	protected void cleanup()
	{
		for (final Map.Entry<BufferManager, List<ByteBuffer>> entry : buffers.entrySet())
		{
			for (final ByteBuffer buf : entry.getValue())
			{
				try
				{
					entry.getKey().put(buf);
				} catch (final IOException e)
				{
				}
			}
			entry.getValue().clear();
		}
	}

	protected abstract void call(RequestResponse response);

	protected abstract void handleError(int error);
}
