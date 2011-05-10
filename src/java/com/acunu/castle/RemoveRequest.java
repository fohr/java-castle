package com.acunu.castle;

import java.nio.ByteBuffer;

/**
 * A request to delete any value associated with the given key.
 */
public final class RemoveRequest extends Request
{
	public final Key key;
	public final int collectionId;

	public final ByteBuffer keyBuffer;

	public RemoveRequest(Key key, int collectionId, ByteBuffer keyBuffer)
	{
		super(CASTLE_RING_REMOVE);

		this.key = key;
		this.collectionId = collectionId;

		/*
		 * Take a slice so the caller can continue to change position/limit in
		 * the original buffer.
		 */
		this.keyBuffer = keyBuffer.slice();
	}

	static private native void copy_to(long buffer, int collectionId, ByteBuffer keyBuffer, int keyOffset, int keyLength);

	protected void copy_to(long buffer) throws CastleException
	{
		int keyLength = key.copyToBuffer(keyBuffer);
		copy_to(buffer, collectionId, keyBuffer, keyBuffer.position(), keyLength);
	}
}
