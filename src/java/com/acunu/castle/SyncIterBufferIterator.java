package com.acunu.castle;

import java.io.IOException;
import java.util.EnumSet;
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
	private boolean hasNext;

	public SyncIterBufferIterator(Castle castle, int collection, Key keyStart, Key keyFinish, EnumSet<IterFlags> flags,
			int bufferSize) throws IOException
	{
		this.castle = castle;
		this.bufferSize = bufferSize;

		IterReply reply = castle.iterstart(collection, keyStart, keyFinish, bufferSize, flags);

		token = reply.token;
		curKvList = reply.elements;
		hasNext = reply.hasNext;
	}

	@Override
	public boolean hasNext()
	{
		if (cancelled)
			return false;

		if (curKvList == null)
		{
			if (!hasNext)
				return false;

			try
			{
				IterReply reply = castle.iternext(token, bufferSize);
				curKvList = reply.elements;
				hasNext = reply.hasNext;
			} catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		}

		if (curKvList.isEmpty())
			return false;

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
}
