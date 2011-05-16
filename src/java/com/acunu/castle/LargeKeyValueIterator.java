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

	/**
	 * @param maxSize
	 *            Inclusive upper bound on value size. Zero means 'unlimited'.
	 * @param numBuffers
	 *            The number of buffers to use for async iterator requests. A
	 *            value of 0 means that only synchronous requests will be made
	 *            (using exactly one buffer).
	 */
	public LargeKeyValueIterator(Castle castle, int collection, Key keyStart, Key keyFinish, int bufferSize,
			long maxSize, int numBuffers, IterFlags flags, StatsRecorder statsRecorder) throws IOException
	{
		super(castle, collection, keyStart, keyFinish, bufferSize, numBuffers, flags, statsRecorder);
		this.collection = collection;
		this.maxSize = maxSize;

		includingValues = (flags != IterFlags.NO_VALUES);
	}

	/**
	 * @param maxSize
	 *            Inclusive upper bound on value size. Zero means 'unlimited'.
	 * @param numBuffers
	 *            The number of buffers to use for async iterator requests. A
	 *            value of 0 means that only synchronous requests will be made
	 *            (using exactly one buffer).
	 */
	public LargeKeyValueIterator(Castle castle, int collection, Key minKey, Key maxKey, Key startKey, int bufferSize,
			long maxSize, int numBuffers, IterFlags flags, StatsRecorder statsRecorder) throws IOException
	{
		super(castle, collection, minKey, maxKey, startKey, bufferSize, numBuffers, flags, statsRecorder);
		this.collection = collection;
		this.maxSize = maxSize;

		includingValues = (flags != IterFlags.NO_VALUES);
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
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
			return next;
		}
	}
}
