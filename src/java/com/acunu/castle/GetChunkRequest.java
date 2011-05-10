package com.acunu.castle;

import java.nio.ByteBuffer;

/**
 * A request to get the next chunk of data from an ongoing big_get operation.
 */
public final class GetChunkRequest extends Request
{
	public final long token;
	public final ByteBuffer chunkBuffer;

	/**
	 * chunkBuffer.position() MUST be page-aligned.
	 */
	public GetChunkRequest(long token, ByteBuffer chunkBuffer)
	{
		super(CASTLE_RING_GET_CHUNK);

		this.token = token;
		/*
		 * Take a slice so the caller can continue to change position/limit in
		 * the original buffer.
		 */
		this.chunkBuffer = chunkBuffer.slice();
	}

	static private native void copy_to(long buffer, long token, ByteBuffer chunkBuffer, int chunkOffset, int chunkLength);

	protected void copy_to(long buffer) throws CastleException
	{
		copy_to(buffer, token, chunkBuffer, chunkBuffer.position(), chunkBuffer.remaining());
	}
}
