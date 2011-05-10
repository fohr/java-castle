package com.acunu.castle;

/**
 * The reply from Castle to a BigGetRequest. found indicates whether any value
 * was found for the key; length indicates the size of that value and is
 * meaningful only if found is true. The token uniquely identifies the big_get
 * operation and is used in subsequent calls to get_chunk.
 */
public class BigGetReply
{
	public final long token;
	public final boolean found;
	public final long length;

	BigGetReply(long token, boolean found, long length)
	{
		this.token = token;
		this.found = found;
		this.length = length;
	}
}
