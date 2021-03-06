package com.acunu.castle;

import java.nio.ByteBuffer;

/**
 * Appends the given data to the end of the value being replaced by a big_put
 * operation.
 */
public final class PutChunkRequest extends Request
{
	public final long token;
	public final ByteBuffer chunkBuffer;

	/**
	 * @param token
	 *            The token returned from a BigPutRequest (in a BigPutReply).
	 * @param chunkBuffer
	 *            remaining() bytes will be read, starting at position().
	 */
	public PutChunkRequest(long token, ByteBuffer chunkBuffer)
	{
		this.token = token;
		/*
		 * Take a slice so the caller can continue to change position/limit in
		 * the original buffer.
		 */
		this.chunkBuffer = chunkBuffer.slice();
	}

	static private native void copy_to(long buffer, int index, long token, ByteBuffer chunkBuffer, int chunkOffset, int chunkLength);

	@Override
	protected void copy_to(long buffer, int index) throws CastleException
	{
		copy_to(buffer, index, token, chunkBuffer, chunkBuffer.position(), chunkBuffer.remaining());
	}
}
