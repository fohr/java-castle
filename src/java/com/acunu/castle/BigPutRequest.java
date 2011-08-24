package com.acunu.castle;

import java.nio.ByteBuffer;

/**
 * Begins a new big_put operation for replacing a key with a large value (for
 * values &le; 512 bytes, use the regular Castle.put() operation).
 */
public final class BigPutRequest extends Request
{
	public final int collectionId;
	public final ByteBuffer keyBuffer;
	public final int keyLength;
	public final long valueLength;

	public BigPutRequest(Key key, int collectionId, ByteBuffer keyBuffer, long valueLength)
	{
		this(collectionId, keyBuffer, copyKey(key, keyBuffer), valueLength);
	}
	
	public BigPutRequest(int collectionId, ByteBuffer keyBuffer, int keyLength, long valueLength)
	{
		super(CASTLE_RING_BIG_PUT);

		// Check there's at least some space for the key
		if (keyBuffer.remaining() == 0)
			throw new IllegalArgumentException("keyBuffer.remaining() is 0");
		if (valueLength < Castle.MIN_BIG_PUT_SIZE)
			throw new IllegalArgumentException("valueLength " + valueLength + " < MIN_BIG_PUT_SIZE "
					+ Castle.MIN_BIG_PUT_SIZE);

		this.collectionId = collectionId;
		this.keyLength = keyLength;
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
	static private native void copy_to(long buffer, int index, int collectionId, ByteBuffer keyBuffer, int keyOffset,
			int keyLength, long valueLength);

	@Override
	protected void copy_to(long buffer, int index) throws CastleException
	{
		copy_to(buffer, index, collectionId, keyBuffer, keyBuffer.position(), keyLength, valueLength);
	}
}
