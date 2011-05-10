package com.acunu.castle;

import java.io.IOException;
import java.util.NoSuchElementException;

import com.acunu.castle.IterStartRequest.IterFlags;

/**
 * KeyValueIterator will return null values for value sizes above a certain
 * (low) limit. LargeKeyValueIterator will as necessary do a get() to ensure all
 * values are returned, up to a specified limit. Any values returned by the
 * underlying iterator found to be larger than this limit will cause an
 * ElementTooLargeException to be thrown. Callers should handle this exception,
 * or ensure values above the limit are never retrieved. Setting limit to zero
 * will attempt to return values of any size (but may fail with an
 * OutOfMemoryError).
 */
public class LargeKeyValueIterator extends KeyValueIterator
{
	private final int collection;
	private final long maxSize;

	private final boolean includingValues;

	private static final int numBuffers = 10;

	/**
	 * @param maxSize
	 *            Inclusive upper bound on value size.
	 */
	public LargeKeyValueIterator(Castle castle, int collection, Key keyStart, Key keyFinish, int bufferSize,
			long maxSize, IterFlags flags, StatsRecorder statsRecorder) throws IOException
	{
		super(castle, collection, keyStart, keyFinish, bufferSize, numBuffers, flags, statsRecorder);
		this.collection = collection;
		this.maxSize = maxSize;

		includingValues = (flags != IterFlags.NO_VALUES);
	}

	/**
	 * Uses the default IterFlags.NONE.
	 */
	public LargeKeyValueIterator(Castle castle, int collection, Key keyStart, Key keyFinish, int bufferSize, long limit)
			throws IOException
	{
		this(castle, collection, keyStart, keyFinish, bufferSize, limit, IterFlags.NONE, null);
	}

	public LargeKeyValueIterator(Castle castle, int collection, Key keyStart, Key keyFinish, int bufferSize,
			long limit, IterFlags flags) throws IOException
	{
		this(castle, collection, keyStart, keyFinish, bufferSize, limit, flags, null);
	}

	public LargeKeyValueIterator(Castle castle, int collection, Key minKey, Key maxKey, Key startKey, int bufferSize,
			long limit, IterFlags flags, StatsRecorder statsRecorder) throws IOException
	{
		super(castle, collection, minKey, maxKey, startKey, bufferSize, numBuffers, flags, statsRecorder);
		this.collection = collection;
		this.maxSize = limit;

		includingValues = (flags != IterFlags.NO_VALUES);
	}

	/**
	 * Uses the default IterFlags.NONE.
	 */
	public LargeKeyValueIterator(Castle castle, int collection, Key minKey, Key maxKey, Key startKey, int bufferSize,
			long limit) throws IOException
	{
		this(castle, collection, minKey, maxKey, startKey, bufferSize, limit, IterFlags.NONE, null);
	}

	public KeyValue next() throws NoSuchElementException, ElementTooLargeException
	{
		KeyValue next = super.next();

		if (!includingValues)
			return next;

		if (next.hasCompleteValue())
			return next;
		else if (maxSize != 0 && next.getValueLength() > maxSize)
			throw new ElementTooLargeException(next.getValueLength(), maxSize);
		else
		{
			try
			{
				next.setValue(castle.get(collection, next.getKey()), next.getValueLength());
			} catch (IOException e)
			{
				throw new RuntimeException(e);
			}
			return next;
		}
	}
}
