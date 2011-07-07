package com.acunu.castle;

import java.io.IOException;
import java.util.NoSuchElementException;

public class TerminatingIterator<K extends Comparable<K>> implements CloseablePeekableIterator<K>
{
	private final CloseablePeekableIterator<K> iter;
	private final K last;

	private boolean hasNext;

	public TerminatingIterator(CloseablePeekableIterator<K> iter, K last)
	{
		this.iter = iter;
		this.last = last;

		cacheHasNext();
	}

	private void cacheHasNext()
	{
		if (!iter.hasNext())
			hasNext = false;
		hasNext = (iter.peek().compareTo(last) <= 0);

		if (!hasNext)
		{
			try
			{
				iter.close();
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}

	@Override
	public K peek()
	{
		if (!hasNext)
			throw new NoSuchElementException();

		return iter.peek();
	}

	@Override
	public boolean hasNext()
	{
		return hasNext;
	}

	@Override
	public K next()
	{
		K next = iter.next();
		cacheHasNext();
		return next;
	}

	@Override
	public void remove()
	{
		iter.remove();
	}

	@Override
	public void close() throws IOException
	{
		iter.close();
	}

}
