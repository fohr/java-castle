package com.acunu.castle;

import java.nio.ByteBuffer;

/**
 * A request to Castle to start a new big_get operation for the given key.
 */
public final class BigGetRequest extends Request
{
	public final Key key;
	public final int collectionId;

	public final ByteBuffer keyBuffer;

	public BigGetRequest(Key key, int collectionId, ByteBuffer keyBuffer)
	{
		super(CASTLE_RING_BIG_GET);

		this.key = key;
		this.collectionId = collectionId;

		/*
		 * Take a slice so the caller can continue to change position/limit in
		 * the original buffer.
		 */
		this.keyBuffer = keyBuffer.slice();
	}

	static private native void copy_to(long buffer, int index, int collectionId, ByteBuffer keyBuffer,
			int keyOffset, int keyLength);

	@Override
	protected void copy_to(long buffer, int index) throws CastleException
	{
		int keyLength = key.copyToBuffer(keyBuffer);
		copy_to(buffer, index, collectionId, keyBuffer, keyBuffer.position(), keyLength);
	}
}
