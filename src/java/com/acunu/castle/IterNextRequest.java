package com.acunu.castle;

import java.nio.ByteBuffer;

/**
 * Retrieves the next batch of KeyValues for a given iterator token.
 */
public final class IterNextRequest extends Request
{
	public final long token;

	public final ByteBuffer buffer;

	public IterNextRequest(long token, ByteBuffer buffer)
	{
		this.token = token;

		/*
		 * Take a slice so the caller can continue to change position/limit in
		 * the original buffer.
		 */
		this.buffer = buffer.slice();
	}

	static private native void copy_to(long buffer, int index, long token, ByteBuffer bbuffer, int bufferOffset, int bufferLength);

	@Override
	protected void copy_to(long target_buffer, int index) throws CastleException
	{
		copy_to(target_buffer, index, token, buffer, buffer.position(), buffer.remaining());
	}
}
