package com.acunu.castle;

import java.io.IOException;
import java.util.EnumSet;
import java.util.NoSuchElementException;

import com.acunu.castle.IterStartRequest.IterFlags;

public class NonTimingOutIterator implements CloseablePeekableIterator<KeyValue>
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
			int numBuffers, EnumSet<IterFlags> flags, StatsRecorder statsRecorder) throws IOException
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
			long maxSize, int numBuffers, EnumSet<IterFlags> flags, StatsRecorder statsRecorder) throws IOException
	{
		iter = new LargeKeyValueIterator(castle, collection, minKey, maxKey, startKey, bufferSize, maxSize, numBuffers,
				EnumSet.of(IterFlags.NONE), null);

		this.castle = castle;
		this.collection = collection;
		this.minKey = minKey;
		this.maxKey = maxKey;
		this.startKey = startKey;
		this.bufferSize = bufferSize;
		this.maxSize = maxSize;
		this.numBuffers = numBuffers;
	}

	protected interface Call<R>
	{
		R call();
	}
	
	protected final Call<Boolean> iterHasNext = new Call<Boolean>()
	{
		@Override
		public Boolean call()
		{
			return iter.hasNext();
		}
	};
	
	protected final Call<KeyValue> iterNext = new Call<KeyValue>()
	{
		@Override
		public KeyValue call()
		{
			KeyValue kv = iter.next();
			if (iter.hasNext())
				startKey = iter.peek().getKey();
			return kv;
		}
	};
	
	protected final Call<KeyValue> iterPeek = new Call<KeyValue>()
	{
		@Override
		public KeyValue call()
		{
			return iter.peek();
		}
	};
	
	protected final Call<Void> iterRemove = new Call<Void>()
	{
		@Override
		public Void call()
		{
			iter.remove();
			return null;
		}
	};
	
	protected <R> R tryCall(Call<R> r)
	{
		try
		{
			return r.call();
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
								numBuffers, EnumSet.of(IterFlags.NONE), null);
						else
							iter = new LargeKeyValueIterator(castle, collection, minKey, maxKey, startKey, bufferSize,
								maxSize, numBuffers, EnumSet.of(IterFlags.NONE), null);
					}
					catch (IOException e1)
					{
						throw new RuntimeException(e1);
					}
					return tryCall(r);
				}
			}
			throw e;
		}
	}
	
	@Override
	public boolean hasNext()
	{
		if (closed)
			return false;

		return tryCall(iterHasNext);
	}

	@Override
	public KeyValue next()
	{
		if (closed)
			throw new NoSuchElementException();

		KeyValue kv = tryCall(iterNext);

		if (!tryCall(iterHasNext))
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
		return tryCall(iterPeek);
	}

	@Override
	public void close() throws IOException
	{
		try
		{
			iter.close();
		} catch(IOException e)
		{
		}
	}

	@Override
	public void remove()
	{
		tryCall(iterRemove);
	}
}
