package com.acunu.castle;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.acunu.castle.IterStartRequest.IterFlags;

public class NonTimingOutIterator implements Iterator<KeyValue>, Closeable
{
	private final Castle castle;
	private final int collection;
	private final Key minKey;
	private final Key maxKey;
	private Key startKey;
	private final int bufferSize;
	private final long maxSize;
	private final int numBuffers;

	private LargeKeyValueIterator iter;
	private boolean closed = false;

	public NonTimingOutIterator(Castle castle, int collection, Key minKey, Key maxKey, int bufferSize, long maxSize,
			int numBuffers, IterFlags flags, StatsRecorder statsRecorder) throws IOException
	{
		iter = new LargeKeyValueIterator(castle, collection, minKey, maxKey, bufferSize, maxSize, numBuffers, flags,
			statsRecorder);

		this.castle = castle;
		this.collection = collection;
		this.minKey = minKey;
		this.maxKey = maxKey;
		this.startKey = null;
		this.bufferSize = bufferSize;
		this.maxSize = maxSize;
		this.numBuffers = numBuffers;
	}

	public NonTimingOutIterator(Castle castle, int collection, Key minKey, Key maxKey, Key startKey, int bufferSize,
			long maxSize, int numBuffers, IterFlags flags, StatsRecorder statsRecorder) throws IOException
	{
		iter = new LargeKeyValueIterator(castle, collection, minKey, maxKey, startKey, bufferSize, maxSize, numBuffers,
			IterFlags.NONE, null);

		this.castle = castle;
		this.collection = collection;
		this.minKey = minKey;
		this.maxKey = maxKey;
		this.startKey = startKey;
		this.bufferSize = bufferSize;
		this.maxSize = maxSize;
		this.numBuffers = numBuffers;
	}

	@Override
	public boolean hasNext()
	{
		if (closed)
			return false;

		try
		{
			return iter.hasNext();
		}
		catch (RuntimeException e)
		{
			Throwable cause = e.getCause();
			if (cause != null && cause instanceof CastleException)
			{
				int errNo = ((CastleException) cause).getErrno();
				if (errNo == -77)
				{
					try
					{
						if (startKey == null)
							iter = new LargeKeyValueIterator(castle, collection, minKey, maxKey, bufferSize, maxSize,
								numBuffers, IterFlags.NONE, null);
						else
							iter = new LargeKeyValueIterator(castle, collection, minKey, maxKey, startKey, bufferSize,
								maxSize, numBuffers, IterFlags.NONE, null);
					}
					catch (IOException e1)
					{
						throw new RuntimeException(e1);
					}
					return hasNext();
				}
			}
			throw e;
		}
	}

	@Override
	public KeyValue next()
	{
		if (closed)
			throw new NoSuchElementException();

		KeyValue kv = iter.next();

		if (iter.hasNext())
			startKey = iter.peek().getKey();
		else
		{
			closed = true;
			try
			{
				iter.close();
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		}

		return kv;
	}

	public KeyValue peek()
	{
		return iter.peek();
	}

	@Override
	public void close() throws IOException
	{
		iter.close();
	}

	@Override
	public void remove()
	{
		iter.remove();
	}
}
