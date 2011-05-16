package com.acunu.castle;

import java.io.IOException;
import java.util.ArrayList;
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
	private volatile Exception exception = null;
	private List<KeyValue> curKvList = null;

	private boolean cancelled = false;
	private int curId = 0;

	public AsyncIterBufferIterator(Castle castle, int collection, Key keyStart, Key keyFinish, IterFlags flags,
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

		token = castle.iterstart(collection, keyStart, keyFinish).token;

		for (int i = 0; i < numBuffers; i++)
			castle.iternext(token, bufferSize, new NextCallback(i));
	}

	private class NextCallback implements IterNextCallback
	{
		private final int id;

		public NextCallback(int id)
		{
			this.id = id;
		}

		@Override
		public void call(IterReply iterReply)
		{
			// we already had an exception so give up
			if (exception != null)
				return;

			try
			{
				kvListArray.get(id).set(iterReply.elements);
			} catch (Exception e)
			{
				setError(e, id);
			}
		}

		@Override
		public void handleError(int error)
		{
			setError(new CastleException(error, "Error in iter_next"), id);
		}
	}

	private void setError(Exception e, int id)
	{
		exception = e;

		/*
		 * Add an empty list to the queue to signal exception
		 * Use offer since if the queue is full we don't need to
		 * wake the user
		 */
		kvListArray.get(id).set(new ArrayList<KeyValue>());
	}

	@Override
	public boolean hasNext()
	{
		if (exception != null)
			throw new RuntimeException(exception);

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
				}
			}
		} finally
		{
			if (interrupted)
				Thread.currentThread().interrupt();
		}

		if (exception != null)
			throw new RuntimeException(exception);

		if (curKvList.isEmpty())
		{
			try
			{
				close();
			} catch (IOException e)
			{
				System.out.println("Exception on iterfinish");
				e.printStackTrace();
			}
			return false;
		} else
		{
			try
			{
				castle.iternext(token, bufferSize, new NextCallback(curId));
			} catch (IOException e)
			{
				throw new RuntimeException(e);
			}

			curId = (curId + 1) % numBuffers;

			return true;
		}
	}

	@Override
	public List<KeyValue> next()
	{
		if (exception != null)
			throw new RuntimeException(exception);

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

		public V get() throws InterruptedException
		{
			synchronized (this)
			{
				if (!set)
					this.wait();

				this.set = false;

				return val;
			}
		}

		public void set(V val)
		{
			synchronized (this)
			{
				this.val = val;
				set = true;
				this.notify();
			}
		}
	}
}
