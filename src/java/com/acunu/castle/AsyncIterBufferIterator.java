package com.acunu.castle;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.NoSuchElementException;

import com.acunu.castle.IterStartRequest.IterFlags;

public class AsyncIterBufferIterator implements IterBufferIterator
{
	private final Castle castle;
	private final int bufferSize;
	private final int numBuffers;
	private final ArrayList<BlockingAtomicReference<List<KeyValue>>> kvListArray;

	private long token;
	private List<KeyValue> curKvList = null;

	private boolean cancelled = false;
	private int curId = 0;
	private volatile boolean hasNext;

	public AsyncIterBufferIterator(Castle castle, int collection, Key keyStart, Key keyFinish, EnumSet<IterFlags> flags,
			int bufferSize, int numBuffers) throws IOException
	{
		if (castle == null)
			throw new IllegalArgumentException("castle");
		if (numBuffers <= 0)
			throw new IllegalArgumentException("numBuffers");

		this.castle = castle;
		this.bufferSize = bufferSize;
		this.numBuffers = numBuffers;

		kvListArray = new ArrayList<BlockingAtomicReference<List<KeyValue>>>(numBuffers);
		for (int i = 0; i < numBuffers; i++)
			kvListArray.add(i, new BlockingAtomicReference<List<KeyValue>>());

		IterReply iterReply = castle.iterstart(collection, keyStart, keyFinish, bufferSize, flags);

		token = iterReply.token;
		kvListArray.get(0).set(iterReply.elements);
		hasNext = iterReply.hasNext;

		for (int i = 1; i < numBuffers; i++)
		{
			// optimisation for if there is just one buffer worth of stuff
			if (!hasNext)
				kvListArray.get(i).set(new ArrayList<KeyValue>());
			else
				castle.iternext(token, bufferSize, new NextCallback(i));
		}
	}

	private class NextCallback implements IterCallback
	{
		private final int id;

		public NextCallback(int id)
		{
			this.id = id;
		}

		@Override
		public void call(IterReply iterReply)
		{
			/*
			 * Be careful with races here (see #4265)
			 * The calls are processed in multiple threads so we have multiple
			 * instances of call running. Ensure we will only ever set it to false
			 * which guarantees races won't hurt us.
			 */
			if (!iterReply.hasNext)
				hasNext = false;
			kvListArray.get(id).set(iterReply.elements);
			assert !iterReply.elements.isEmpty();
		}

		@Override
		public void handleError(int error)
		{
			// Call set to notify. Must be called last.
			kvListArray.get(id).setError(error);
			kvListArray.get(id).set(new ArrayList<KeyValue>());
		}
	}

	@Override
	public boolean hasNext()
	{
		if (cancelled)
			return false;

		if (curKvList != null)
			return !curKvList.isEmpty();

		// we want to set the thread interrupted status if we got interrupted
		boolean interrupted = false;

		try
		{
			while (true)
			{
				try
				{
					curKvList = kvListArray.get(curId).get();
					break;
				} catch (InterruptedException e)
				{
					interrupted = true;
					System.out.println("InterruptedException waiting for element in queue");
					e.printStackTrace();
				} catch (CastleException e)
				{
					// swallow if we got to the end - most likely it will
					// be a token not found error. But we don't care since we
					// got all the data
					if (hasNext)
						throw new RuntimeException(e);
					else
						break;
				}
			}
		} finally
		{
			if (interrupted)
				Thread.currentThread().interrupt();
		}

		if (curKvList == null || curKvList.isEmpty())
			return false;

		// don't bother to call iternext if we already know there are none left
		if (!hasNext)
			kvListArray.get(curId).set(new ArrayList<KeyValue>());
		else
		{

			try
			{
				castle.iternext(token, bufferSize, new NextCallback(curId));
			} catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		}

		curId = (curId + 1) % numBuffers;

		return true;
	}

	@Override
	public List<KeyValue> next()
	{
		if (!hasNext())
			throw new NoSuchElementException();

		List<KeyValue> next = curKvList;
		curKvList = null;

		return next;
	}

	@Override
	public void remove()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() throws IOException
	{
		if (cancelled)
			return;

		cancelled = true;

		// only call iterfinish if we terminated early
		if (hasNext)
			castle.iterfinish(token);
	}

	@Override
	public void finalize() throws IOException
	{
		close();
	}

	private static class BlockingAtomicReference<V>
	{
		private boolean set = false;
		private V val = null;
		private int error = 0;

		public synchronized V get() throws InterruptedException, CastleException
		{
			if (!set)
				this.wait();

			this.set = false;
			int err = error;
			error = 0;

			if (err != 0)
				throw new CastleException(err, "Error during iter_next");

			return val;
		}

		public synchronized void set(V val)
		{
			this.val = val;
			set = true;
			this.notify();
		}

		public synchronized void setError(final int error)
		{
			this.error = error;
		}
	}
}
