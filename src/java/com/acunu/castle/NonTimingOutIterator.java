package com.acunu.castle;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class NonTimingOutIterator implements Iterator<KeyValue>, Closeable
{
	private static final int bufferSize = 1024 * 1024;

	private final Castle castle;
	private final int collection;
	private final Key minKey;
	private final Key maxKey;
	private Key startKey;

	private LargeKeyValueIterator iter;
	private boolean closed = false;

	public NonTimingOutIterator(Castle castle, int collection, Key minKey, Key maxKey) throws IOException
	{
		iter = new LargeKeyValueIterator(castle, collection, minKey, maxKey, bufferSize, 0);

		this.castle = castle;
		this.collection = collection;
		this.minKey = minKey;
		this.maxKey = maxKey;
		this.startKey = null;
	}

	public NonTimingOutIterator(Castle castle, int collection, Key minKey, Key maxKey, Key startKey) throws IOException
	{
		iter = new LargeKeyValueIterator(castle, collection, minKey, maxKey, startKey, bufferSize, 0);

		this.castle = castle;
		this.collection = collection;
		this.minKey = minKey;
		this.maxKey = maxKey;
		this.startKey = startKey;
	}

	@Override
	public boolean hasNext()
	{
		if (closed)
			return false;

		try
		{
			return iter.hasNext();
		} catch (RuntimeException e)
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
							iter = new LargeKeyValueIterator(castle, collection, minKey, maxKey, bufferSize, 0);
						else
							iter = new LargeKeyValueIterator(castle, collection, minKey, maxKey, startKey, bufferSize,
									0);
					} catch (IOException e1)
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
			} catch (IOException e)
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
