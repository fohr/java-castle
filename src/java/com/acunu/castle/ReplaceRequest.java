package com.acunu.castle;

import java.nio.ByteBuffer;

/**
 * A request to replace the given key with the given value.
 */
public final class ReplaceRequest extends Request
{
	public final int collectionId;
	public final int keyLength;

	public final ByteBuffer keyBuffer;
	public final ByteBuffer valueBuffer;
	
	/**
	 * @param keyBuffer
	 *            Buffer to copy key to. Key is copied to the Buffer's current
	 *            position().
	 * @throws CastleException 
	 */
	public ReplaceRequest(Key key, int collectionId, ByteBuffer keyBuffer, ByteBuffer valueBuffer) throws CastleException
	{
		this(collectionId, keyBuffer, copyKey(key, keyBuffer), valueBuffer);
	}
	
	public ReplaceRequest(int collectionId, ByteBuffer keyBuffer, int keyLength, ByteBuffer valueBuffer)
	{
		super(CASTLE_RING_REPLACE);

		this.collectionId = collectionId;
		this.keyLength = keyLength;
		/*
		 * Take a slice so the caller can continue to change position/limit in
		 * the original buffer.
		 */
		this.keyBuffer = keyBuffer.slice();
		this.valueBuffer = valueBuffer.slice();
	}

	static private native void copy_to(long buffer, int index, int collectionId, ByteBuffer keyBuffer, int keyOffset,
			int keyLength, ByteBuffer valueBuffer, int valueOffset, int valueLength);

	protected void copy_to(long buffer, int index) throws CastleException
	{
		copy_to(buffer, index, collectionId, keyBuffer, keyBuffer.position(), keyLength, valueBuffer, valueBuffer.position(),
				valueBuffer.remaining());
	}
}
