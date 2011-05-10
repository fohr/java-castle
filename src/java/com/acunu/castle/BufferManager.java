package com.acunu.castle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class BufferManager
{
	private final Castle castle;
	private boolean closed;

	private final Map<Integer, LinkedList<ByteBuffer>> buffers;
	private final Map<Integer, Integer> sizeCountMap;
	private final List<Integer> sizes;
	private final boolean blocking;

	BufferManager(final Castle castle) throws IOException
	{
		this(castle, new HashMap<Integer, Integer>());
	}

	BufferManager(final Castle castle, final Map<Integer, Integer> sizeCountMap) throws IOException
	{
		this(castle, sizeCountMap, false);
	}

	BufferManager(final Castle castle, final Map<Integer, Integer> sizeCountMap, final boolean blocking)
	        throws IOException
	{
		this.castle = castle;
		this.sizeCountMap = sizeCountMap;
		this.blocking = blocking;

		buffers = new HashMap<Integer, LinkedList<ByteBuffer>>(sizeCountMap.size());
		sizes = new ArrayList<Integer>(sizeCountMap.size());

		for (final Entry<Integer, Integer> entry : sizeCountMap.entrySet())
		{
			final LinkedList<ByteBuffer> bufferList = new LinkedList<ByteBuffer>();

			final int size = entry.getKey();

			for (int i = 0; i < entry.getValue(); i++)
			{
				bufferList.add(castle.createBuffer(size));
			}

			buffers.put(size, bufferList);
			sizes.add(size);
		}

		Collections.sort(sizes);
	}

	@Override
	protected void finalize() throws IOException
	{
		close();
	}

	void close() throws IOException
	{
		if (closed)
			return;

		IOException exn = null;
		for (final Entry<Integer, LinkedList<ByteBuffer>> entry : buffers.entrySet())
		{
			for (final ByteBuffer buffer : entry.getValue())
			{
				try
				{
					castle.destroyBuffer(buffer);
				} catch (final IOException e)
				{
					exn = e;
				}
			}
		}
		closed = true;
		if (exn != null)
			throw exn;
	}

	public ByteBuffer get(final int size) throws IOException
	{
		if (closed)
			throw new IOException("BufferManager is closed");

		if (size > Castle.MAX_BUFFER_SIZE)
			throw new IllegalArgumentException("Buffer size requested greater than Castle.MAX_BUFFER_SIZE");
		if (size == 0)
			return ByteBuffer.allocateDirect(0);

		if (sizeCountMap.isEmpty())
			return castle.createBuffer(size);

		// find the next largest size we have
		int sizeToUse = 0;
		for (final int availableSize : sizes)
		{
			if (availableSize >= size)
			{
				sizeToUse = availableSize;
				break;
			}
		}

		// if there aren't any big enough
		if (sizeToUse == 0)
			return castle.createBuffer(size);

		final LinkedList<ByteBuffer> bufferList = buffers.get(sizeToUse);
		ByteBuffer buffer = null;

		synchronized (bufferList)
		{
			if (blocking)
			{
				while (bufferList.isEmpty())
				{
					try
					{
						bufferList.wait();
					} catch (final InterruptedException e)
					{
					}
				}
				buffer = bufferList.remove();
			} else if (!bufferList.isEmpty())
				buffer = bufferList.remove();
		}

		if (buffer == null)
			return castle.createBuffer(size);

		buffer.clear();
		buffer.limit(size);
		return buffer;
	}

	public void put(final ByteBuffer buffer) throws IOException
	{
		if (closed)
			throw new IOException("BufferManager is closed");

		if (buffer.capacity() == 0)
			return;

		if (sizeCountMap.isEmpty())
		{
			castle.destroyBuffer(buffer);
			return;
		}

		final LinkedList<ByteBuffer> bufferList = buffers.get(buffer.capacity());
		if (bufferList == null)
		{
			castle.destroyBuffer(buffer);
			return;
		}

		synchronized (bufferList)
		{
			if (bufferList.size() < sizeCountMap.get(buffer.capacity()))
			{
				bufferList.add(buffer);
				if (blocking)
					bufferList.notify();
				return;
			}
		}
		castle.destroyBuffer(buffer);
	}
}
