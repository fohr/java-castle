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
	
	public final Long timestamp;

	public RemoveRequest(Key key, int collectionId, ByteBuffer keyBuffer, Long timestamp)
	{
		this.key = key;
		this.collectionId = collectionId;

		/*
		 * Take a slice so the caller can continue to change position/limit in
		 * the original buffer.
		 */
		this.keyBuffer = keyBuffer.slice();
		this.timestamp = timestamp;
	}

	static private native void copy_to(
			long buffer, int index, int collectionId, ByteBuffer keyBuffer, int keyOffset, int keyLength,
			long timestamp, boolean useTimestamp
	);

	@Override
	protected void copy_to(long buffer, int index) throws CastleException
	{
		int keyLength = key.copyToBuffer(keyBuffer);
		copy_to(buffer, index, collectionId, keyBuffer, keyBuffer.position(), keyLength, 
				timestamp == null ? 0 : timestamp, timestamp != null);
	}
}
