package com.acunu.castle;

public class ElementTooLargeException extends RuntimeException
{
	private final long limit, size;

	public ElementTooLargeException(long size, long limit)
	{
		this.size = size;
		this.limit = limit;
	}

	public long getSize()
	{
		return size;
	}

	public long getLimit()
	{
		return limit;
	}

	@Override
	public String toString()
	{
		return "ElementTooLargeException(size=" + size + ", limit=" + limit + ")";
	}
}
