package com.acunu.castle;

/**
 * The reply from Castle to a BigPutRequest. The token provided here uniquely
 * identifies the big_put operation and is used in subsequent calls to
 * put_chunk.
 */
public class BigPutReply
{
	public final long token;

	BigPutReply(long token)
	{
		this.token = token;
	}
}
