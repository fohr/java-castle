package com.acunu.castle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public abstract class AbstractCallback implements Runnable
{
	protected final Map<BufferManager, List<ByteBuffer>> buffers = new HashMap<BufferManager, List<ByteBuffer>>();
	
	protected abstract void process(); 
	
	public void run()
	{
		try
		{
			process();
		} finally
		{
			cleanup();
		}
	}
	
	
	/* package private */
	void collect(final BufferManager manager, final ByteBuffer... bufs)
	{
		if (!buffers.containsKey(manager))
			buffers.put(manager, new LinkedList<ByteBuffer>());
		buffers.get(manager).addAll(Arrays.asList(bufs));
	}

	public void collect(final Castle castle, final ByteBuffer... bufs)
	{
		collect(castle.getBufferManager(), bufs);
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
}
