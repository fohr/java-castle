package com.acunu.castle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.acunu.castle.IterStartRequest.IterFlags;

public class CachingKeyValueIterator implements SeekableIterator<KeyValue, Key>
{
	private final Castle castle;
	private final int collection;
	private final Key keyStart;
	private final Key keyFinish;
	private final long cacheSize;

	private NonTimingOutIterator kvIter;
	private PeekableIterator<KeyValue> cacheIter = null;
	private ArrayList<KeyValue> kvCache = new ArrayList<KeyValue>();
	private long cacheUsed = 0;

	public CachingKeyValueIterator(Castle castle, int collection, Key keyStart, Key keyFinish, long cacheSize)
			throws IOException
	{
		this.castle = castle;
		this.collection = collection;
		this.keyStart = keyStart;
		this.keyFinish = keyFinish;
		this.cacheSize = cacheSize;

		kvIter = new NonTimingOutIterator(castle, collection, keyStart, keyFinish, 1024 * 1024, 0, 10, IterFlags.NONE,
			null);

		resetCache();
	}

	private void resetCache()
	{
		cacheUsed = 0;
		kvCache.clear();
		cacheIter = null;
	}

	@Override
	public void close() throws IOException
	{
		kvIter.close();
		resetCache();
	}

	public KeyValue peek() throws NoSuchElementException
	{
		if (cacheIter != null && cacheIter.hasNext())
			return cacheIter.peek();

		return kvIter.peek();
	}

	@Override
	public boolean hasNext()
	{
		if (cacheIter != null)
			return cacheIter.hasNext() || kvIter.hasNext();

		return kvIter.hasNext();
	}

	@Override
	public KeyValue next()
	{
		if (cacheIter != null && cacheIter.hasNext())
			return cacheIter.next();

		KeyValue kv = kvIter.next();

		// if go beyond the end of the rollback then reset the cache
		if (cacheIter != null && !cacheIter.hasNext())
			resetCache();

		// add kv to the cache if it fits
		long size = kv.getKey().getApproximateLength() + kv.getValueLength();
		if (size + cacheUsed > cacheSize)
			resetCache();

		cacheUsed += size;
		kvCache.add(kv);

		return kv;
	}

	/**
	 * Roll back the iterator to the key one after the last time you rolled
	 * back. If the first rollback, will go back to the start.
	 * 
	 * Reads from cache if can, otherwise restarts the iterator
	 */
	public void rollback(Key rollbackKey) throws IOException
	{
		cacheIter = new PeekableIterator<KeyValue>(kvCache.iterator());

		if (!cacheIter.hasNext() || cacheIter.peek().getKey().compareTo(rollbackKey) > 0)
		{
			// don't use the cacheIter since we've overflowed cache
			resetCache();
			kvIter.close();
			kvIter = new NonTimingOutIterator(castle, collection, keyStart, keyFinish, rollbackKey, 1024 * 1024, 0, 10,
				IterFlags.NONE, null);
		}
		else
		{
			// TODO: could speed this up for large caches using binary search
			while (cacheIter.hasNext())
			{
				if (cacheIter.peek().getKey().compareTo(rollbackKey) < 0)
					cacheIter.next();
				else
					break;
			}
		}
	}

	@Override
	public void remove()
	{
		throw new UnsupportedOperationException();
	}

	private static class PeekableIterator<T> implements Iterator<T>
	{
		private final Iterator<T> iter;
		private T peekValue = null;

		public PeekableIterator(Iterator<T> iter)
		{
			this.iter = iter;
		}

		@Override
		public boolean hasNext()
		{
			if (peekValue != null)
				return true;
			return iter.hasNext();
		}

		public T peek()
		{
			peekValue = next();
			return peekValue;
		}

		@Override
		public T next()
		{
			if (peekValue != null)
			{
				T val = peekValue;
				peekValue = null;
				return val;
			}

			return iter.next();
		}

		@Override
		public void remove()
		{
			iter.remove();
		}
	}

}
