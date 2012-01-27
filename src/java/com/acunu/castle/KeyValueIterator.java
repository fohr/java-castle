package com.acunu.castle;

import java.io.Closeable;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.acunu.castle.IterStartRequest.IterFlags;

/**
 * Iterates across all KeyValues found in the hypercube specified by a min and
 * max key. An optional startKey specifies where the iterator should start.
 */
public class KeyValueIterator implements CloseablePeekableIterator<KeyValue>
{
	protected final Castle castle;
	private final int collection;
	private final int bufferSize;
	private final int numBuffers;
	private final Key minKey;
	private final Key maxKey;
	private final Key startKey;
	private int nextStartKeyDim;
	private final EnumSet<IterFlags> flags;
	private final StatsRecorder statsRecorder;
	private final boolean synchronous;

	private IterBufferIterator bufferIter = null;
	private boolean closed;
	private Iterator<KeyValue> batchIterator;
	private KeyValue peekValue;

	KeyValueIterator(Castle castle, int collection, Key minKey, Key maxKey, int bufferSize, int numBuffers,
			boolean synchronous, EnumSet<IterFlags> flags, StatsRecorder statsRecorder) throws IOException
	{
		this.statsRecorder = statsRecorder;
		this.castle = castle;
		this.collection = collection;
		this.bufferSize = bufferSize;
		this.numBuffers = numBuffers;
		this.synchronous = synchronous;
		this.minKey = minKey;
		this.maxKey = maxKey;
		this.flags = flags;

		if (synchronous)
			bufferIter = new SyncIterBufferIterator(castle, collection, minKey, maxKey, flags, bufferSize);
		else
			bufferIter = new AsyncIterBufferIterator(castle, collection, minKey, maxKey, flags, bufferSize, numBuffers);

		// Not used in this case
		this.startKey = null;
		this.nextStartKeyDim = -1;
	}

	public KeyValueIterator(Castle castle, int collection, Key minKey, Key maxKey, int bufferSize, EnumSet<IterFlags> flags,
			StatsRecorder statsRecorder) throws IOException
	{
		this(castle, collection, minKey, maxKey, bufferSize, 0, true, flags, statsRecorder);
	}

	/**
	 * @param numBuffers
	 *            The number of buffers to use for async iterator requests. A
	 *            value of 0 means that only synchronous requests will be made
	 *            (using exactly one buffer).
	 */
	public KeyValueIterator(Castle castle, int collection, Key minKey, Key maxKey, int bufferSize, int numBuffers,
			EnumSet<IterFlags> flags, StatsRecorder statsRecorder) throws IOException
	{
		this(castle, collection, minKey, maxKey, bufferSize, numBuffers, numBuffers <= 0, flags, statsRecorder);
	}

	public KeyValueIterator(Castle castle, int collection, Key minKey, Key maxKey, int bufferSize, EnumSet<IterFlags> flags)
			throws IOException
	{
		this(castle, collection, minKey, maxKey, bufferSize, flags, null);
	}

	public KeyValueIterator(Castle castle, int collection, Key minKey, Key maxKey, int bufferSize, int numBuffers,
			EnumSet<IterFlags> flags) throws IOException
	{
		this(castle, collection, minKey, maxKey, bufferSize, numBuffers, flags, null);
	}

	/**
	 * Just uses the default IterFlags.
	 */

	public KeyValueIterator(Castle castle, int collection, Key minKey, Key maxKey, int bufferSize) throws IOException
	{
		this(castle, collection, minKey, maxKey, bufferSize, EnumSet.of(IterFlags.NONE));
	}

	public KeyValueIterator(Castle castle, int collection, Key minKey, Key maxKey, int bufferSize, int numBuffers)
			throws IOException
	{
		this(castle, collection, minKey, maxKey, bufferSize, numBuffers, EnumSet.of(IterFlags.NONE));
	}

	public KeyValueIterator(Castle castle, int collection, Key minKey, Key maxKey, Key startKey, int bufferSize,
			EnumSet<IterFlags> flags, StatsRecorder statsRecorder) throws IOException
	{
		this(castle, collection, minKey, maxKey, startKey, bufferSize, 0, true, flags, statsRecorder);
	}

	/**
	 * @param numBuffers
	 *            The number of buffers to use for async iterator requests. A
	 *            value of 0 means that only synchronous requests will be made
	 *            (using exactly one buffer).
	 */
	public KeyValueIterator(Castle castle, int collection, Key minKey, Key maxKey, Key startKey, int bufferSize,
			int numBuffers, EnumSet<IterFlags> flags, StatsRecorder statsRecorder) throws IOException
	{
		this(castle, collection, minKey, maxKey, startKey, bufferSize, numBuffers, numBuffers <= 0, flags,
			statsRecorder);
	}

	/**
	 * Takes an additional startKey param; returns an iterator that behaves as a
	 * range query on minKey-maxKey, but starting at the given startKey. See
	 * trac-1718.
	 */
	KeyValueIterator(Castle castle, int collection, Key minKey, Key maxKey, Key startKey, int bufferSize,
			int numBuffers, boolean synchronous, EnumSet<IterFlags> flags, StatsRecorder statsRecorder) throws IOException
	{
		checkKeyDimensions(minKey, maxKey, startKey);
		checkValidStartKey(startKey);
		this.castle = castle;
		this.collection = collection;
		this.minKey = minKey;
		this.maxKey = maxKey.clone();
		if (minKey.compareTo(startKey) == 0)
		{
			/* Equivalent to the 2-arg constructor. */
			this.startKey = null;
			if (synchronous)
				bufferIter = new SyncIterBufferIterator(castle, collection, minKey, maxKey, flags, bufferSize);
			else
				bufferIter = new AsyncIterBufferIterator(castle, collection, minKey, maxKey, flags, bufferSize,
					numBuffers);
		}
		else
		{
			this.startKey = startKey.clone();
			this.nextStartKeyDim = this.startKey.getDimensions() - 1;
		}
		this.bufferSize = bufferSize;
		this.numBuffers = numBuffers;
		this.synchronous = synchronous;
		this.flags = flags;
		this.statsRecorder = statsRecorder;
		startNextIter();
	}

	private void startNextIter() throws IOException
	{
		if (startKey == null)
		{
			/*
			 * This means we're doing a regular range query with no start
			 * offset.
			 */
			return;
		}

		/* Set to sentinel value. */
		this.bufferIter = null;

		/*
		 * We decompose the original query into several range queries. e.g. the
		 * query [a,b,c,d]-[w,x,y,z] starting at [m,n,o,p] consists of the
		 * four components [m,n,o,p]-[m,n,o,z], [m,n,o+ε,d]-[m,n,y,z],
		 * [m,n+ε,c,d]-[m,x,y,z], [m+ε,b,c,d]-[w,x,y,z].
		 */
		if (nextStartKeyDim >= 0)
		{
			/* set up the end key. */
			Key newMax = maxKey.clone();
			for (int j = 0; j < nextStartKeyDim; j++)
			{
				if (startKey.key[j].length == 0)
					newMax.key[j] = new byte[]{0};
				else
					newMax.key[j] = startKey.key[j].clone();
			}

			if (synchronous)
				bufferIter = new SyncIterBufferIterator(castle, collection, startKey, newMax, flags, bufferSize);
			else
				bufferIter = new AsyncIterBufferIterator(castle, collection, startKey, newMax, flags, bufferSize,
						numBuffers);
			
			/* Set up the start key for the next range query: */
			startKey.key[nextStartKeyDim] = minKey.key[nextStartKeyDim].clone();
			--nextStartKeyDim;
			if (nextStartKeyDim >= 0)
			{
				/*
				 * Increment the preceding dimension by epsilon so that we don't
				 * double up on the next query (start and end keys are both
				 * inclusive). This means adding a zero byte to the end of the
				 * dim.
				 */
				byte[] plusEpsilon = new byte[startKey.key[nextStartKeyDim].length + 1];
				System.arraycopy(startKey.key[nextStartKeyDim], 0, plusEpsilon, 0, startKey.key[nextStartKeyDim].length);
				/* The last byte will be zero by default. */
				startKey.key[nextStartKeyDim] = plusEpsilon;
			}
		}
	}

	/**
	 * Checks that each of the given keys has the same number of dimensions.
	 */
	private static void checkKeyDimensions(Key... keys)
	{
		int dims = keys[0].getDimensions();
		for (Key key : keys)
		{
			if (key.getDimensions() != dims)
				throw new IllegalArgumentException("Number of dimensions differs between given keys ("
						+ key.getDimensions() + " != " + dims + ")");
		}
	}

	/**
	 * Checks that no concrete dimension given after the first infinite one.
	 */
	private static void checkValidStartKey(Key key)
	{
		boolean emptyReached = false;
		for (byte[] dim : key.key)
		{
			if (emptyReached && dim.length != 0)
				throw new IllegalArgumentException(
						"In iterator start key, concrete dimension given after infinite dimension: " + key);
			emptyReached = dim.length == 0;
		}
	}

	@Override
	protected void finalize() throws Throwable
	{
		this.close();
	}

	@Override
	public boolean hasNext()
	{
		return hasNextInBatch();
	}

	/** We may need to pull in the next batch to see if there's any more. */
	private boolean hasNextInBatch()
	{
		if (closed)
			return false;

		if (peekValue != null)
			return true;

		if (batchIterator != null && batchIterator.hasNext())
			return true;

		// none in the buffer, see if we can get more
		while (bufferIter != null)
		{
			long startTime = System.nanoTime();
			boolean hasNext = bufferIter.hasNext();
			if (statsRecorder != null)
				statsRecorder.recordLatency(startTime);

			// if get nothing then we're at the end
			if (!hasNext)
			{
				// Close old iterator that we've now finished with.
				try
				{
					closeToken();
					startNextIter();
				} catch (IOException e)
				{
					throw new RuntimeException(e);
				}
			} else
			{
				List<KeyValue> nextBatch = bufferIter.next();
				batchIterator = nextBatch.listIterator();
				return true;
			}
		}

		try
		{
			close();
		} catch (IOException e)
		{
			throw new RuntimeException(e);
		}
		return false;
	}

	public KeyValue next() throws NoSuchElementException
	{
		if (!hasNextInBatch())
			throw new NoSuchElementException();

		KeyValue kv;
		if (peekValue != null)
		{
			kv = peekValue;
			peekValue = null;
		} else
			kv = batchIterator.next();

		return kv;
	}

	public KeyValue peek() throws NoSuchElementException
	{
		if (peekValue == null)
			peekValue = next();
		return peekValue;
	}

	public synchronized void close() throws IOException
	{
		if (closed)
			return;

		closed = true;
		batchIterator = null;
		peekValue = null;
		closeToken();
	}

	/**
	 * Closes any iterator associated with the current token. Does nothing if
	 * token == -1.
	 */
	private void closeToken() throws IOException
	{
		if (bufferIter == null)
			return;

		try
		{
			long startTime = System.nanoTime();
			bufferIter.close();
			if (statsRecorder != null)
				statsRecorder.recordLatency(startTime);
		} catch (CastleException e)
		{
			/*
			 * -EBADFD means your token has expired. This isn't a problem, since it
			 * might have just timed out.
			 * -EINVAL means you may have queued up multiple iter_finishes, and this
			 * one is getting cancelled.
			 */
			if (e.getErrno() != -77 && e.getErrno() != -22)
				throw new IOException("Closing iterator failed", e);
		}
		bufferIter = null;
	}

	/**
	 * Not implemented.
	 */
	@Override
	public void remove()
	{
		throw new UnsupportedOperationException();
	}
}