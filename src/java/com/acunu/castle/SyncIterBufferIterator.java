package com.acunu.castle;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import com.acunu.castle.IterStartRequest.IterFlags;

public class SyncIterBufferIterator implements IterBufferIterator
{
	private final Castle castle;
	private final int bufferSize;

	private final long token;

	private boolean cancelled;
	private List<KeyValue> curKvList = null;

	public SyncIterBufferIterator(Castle castle, int collection, Key keyStart, Key keyFinish, IterFlags flags,
			int bufferSize) throws IOException
	{
		this.castle = castle;
		this.bufferSize = bufferSize;

		token = castle.iterstart(collection, keyStart, keyFinish, flags).token;
	}

	@Override
	public boolean hasNext()
	{
		if (cancelled)
			return false;

		if (curKvList == null)
		{
			try
			{
				curKvList = castle.iternext(token, bufferSize).elements;
			} catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		}

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
		}

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

		castle.iterfinish(token);
	}

	@Override
	public void finalize() throws IOException
	{
		close();
	}
}
