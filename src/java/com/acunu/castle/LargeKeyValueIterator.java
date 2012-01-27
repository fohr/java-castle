package com.acunu.castle;

import java.io.IOException;
import java.util.EnumSet;
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
			long maxSize, int numBuffers, EnumSet<IterFlags> flags, StatsRecorder statsRecorder) throws IOException
	{
		super(castle, collection, keyStart, keyFinish, bufferSize, numBuffers, setGetOol(flags), statsRecorder);
		this.collection = collection;
		this.maxSize = maxSize;

		includingValues = !flags.contains(IterFlags.NO_VALUES);
	}

	public static EnumSet<IterFlags> setGetOol(EnumSet<IterFlags> flags)
	{
		flags.add(IterFlags.GET_OOL);
		return flags;
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
			long maxSize, int numBuffers, EnumSet<IterFlags> flags, StatsRecorder statsRecorder) throws IOException
	{
		super(castle, collection, minKey, maxKey, startKey, bufferSize, numBuffers, setGetOol(flags), statsRecorder);
		this.collection = collection;
		this.maxSize = maxSize;

		includingValues = !flags.contains(IterFlags.NO_VALUES);
	}

	@Override
	public boolean hasNext()
	{
		// if peek succeeds then we have next, else we don't
		try
		{
			peek();
			return true;
		} catch (NoSuchElementException e)
		{
			return false;
		}
	}

	public KeyValue next() throws NoSuchElementException, ElementTooLargeException
	{
		// loop round, so we can skip over null values that were deleted
		// after the iterator started
		while (true)
		{
			// if we get to the end before getting a non-null key, we will get
			// NoSuchElementException here
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
					KeyValue kv = castle.get_kv(collection, next.getKey());
					if (kv != null)
						return kv;
				} catch (IOException e)
				{
					throw new RuntimeException(e);
				}
			}
		}
	}
}
