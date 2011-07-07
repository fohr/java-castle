package com.acunu.castle;

import java.io.IOException;
import java.util.Iterator;

public class IteratorUtils
{
	
	static class PeekIterator<T> implements CloseablePeekableIterator<T>
	{
		T next = null;
		final Iterator<T> iter;
		
		PeekIterator(final Iterator<T> iter)
		{
			this.iter = iter;
		}
		
		@Override
		public boolean hasNext()
		{
			return next != null || iter.hasNext();
		}

		@Override
		public T next()
		{
			if (next != null)
			{
				T tmp = next;
				next = null;
				return tmp;
			}
			return iter.next();
		}
		
		@Override
		public T peek()
		{
			if (next != null)
				return next;
			if (!iter.hasNext())
				return null;
			next = iter.next();
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
		}
	}
	
	public static <T> CloseablePeekableIterator<T> getPeekableIterator(Iterator<T> iter)
	{
		return new PeekIterator<T>(iter);
	}
}
