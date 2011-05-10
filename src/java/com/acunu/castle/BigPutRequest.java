package com.acunu.castle;

import java.nio.ByteBuffer;

/**
 * Begins a new big_put operation for replacing a key with a large value (for
 * values &le; 512 bytes, use the regular Castle.put() operation).
 */
public final class BigPutRequest extends Request
{
	public final Key key;
	public final int collectionId;
	public final ByteBuffer keyBuffer;
	public final long valueLength;

	public BigPutRequest(Key key, int collectionId, ByteBuffer keyBuffer, long valueLength)
	{
		super(CASTLE_RING_BIG_PUT);

		// Check there's at least some space for the key
		if (keyBuffer.remaining() == 0)
			throw new IllegalArgumentException("keyBuffer.remaining() is 0");
		if (valueLength < Castle.MIN_BIG_PUT_SIZE)
			throw new IllegalArgumentException("valueLength " + valueLength + " < MIN_BIG_PUT_SIZE "
					+ Castle.MIN_BIG_PUT_SIZE);

		this.key = key;
		this.collectionId = collectionId;
		/*
		 * Take a slice so the caller can continue to change position/limit in
		 * the original buffer.
		 */
		this.keyBuffer = keyBuffer.slice();
		this.valueLength = valueLength;
	}

	/**
	 * Does NOT affect the position, limit etc. of keyBuffer.
	 */
	static private native void copy_to(long buffer, int collectionId, ByteBuffer keyBuffer, int keyOffset,
			int keyLength, long valueLength);

	protected void copy_to(long buffer) throws CastleException
	{
		int keyLength = key.copyToBuffer(keyBuffer);
		copy_to(buffer, collectionId, keyBuffer, keyBuffer.position(), keyLength, valueLength);
	}
}
