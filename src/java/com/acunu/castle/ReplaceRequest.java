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
	
	public final Long timestamp;
	
	/**
	 * @param keyBuffer
	 *            Buffer to copy key to. Key is copied to the Buffer's current
	 *            position().
	 * @throws CastleException 
	 */
	public ReplaceRequest(Key key, int collectionId, ByteBuffer keyBuffer, ByteBuffer valueBuffer) throws CastleException
	{
		this(key, collectionId, keyBuffer, valueBuffer, null);
	}
	
	public ReplaceRequest(Key key, int collectionId, ByteBuffer keyBuffer, ByteBuffer valueBuffer, Long timestamp) throws CastleException
	{
		this(collectionId, keyBuffer, copyKey(key, keyBuffer), valueBuffer, timestamp);
	}
	
	public ReplaceRequest(int collectionId, ByteBuffer keyBuffer, int keyLength, ByteBuffer valueBuffer)
	{
		this(collectionId, keyBuffer, keyLength, valueBuffer, null);
	}
	
	public ReplaceRequest(int collectionId, ByteBuffer keyBuffer, int keyLength, ByteBuffer valueBuffer, Long timestamp)
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
		this.timestamp = timestamp;
	}

	static private native void copy_to(long buffer, int index, int collectionId, ByteBuffer keyBuffer, int keyOffset,
			int keyLength, ByteBuffer valueBuffer, int valueOffset, int valueLength, long timestamp, boolean useTimestamp);

	protected void copy_to(long buffer, int index) throws CastleException
	{
		copy_to(buffer, index, collectionId, keyBuffer, keyBuffer.position(), keyLength, valueBuffer, valueBuffer.position(),
				valueBuffer.remaining(), timestamp == null ? 0 : timestamp, timestamp != null);
	}
}
