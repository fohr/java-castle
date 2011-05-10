package com.acunu.castle;

import java.nio.ByteBuffer;

/**
 * A request to get any value associated with a single key. Will only return
 * values <= 1 MB; for larger values, use a big_get.
 */
public final class GetRequest extends Request
{
	public final Key key;
	public final int collectionId;

	public final ByteBuffer keyBuffer;
	public final ByteBuffer valueBuffer;

	public GetRequest(Key key, int collectionId, ByteBuffer keyBuffer, ByteBuffer valueBuffer)
	{
		super(CASTLE_RING_GET);

		this.key = key;
		this.collectionId = collectionId;

		/*
		 * Take a slice so the caller can continue to change position/limit in
		 * the original buffer.
		 */
		this.keyBuffer = keyBuffer.slice();
		this.valueBuffer = valueBuffer.slice();
	}

	static private native void copy_to(long buffer, int collectionId, ByteBuffer keyBuffer, int keyOffset,
			int keyLength, ByteBuffer valueBuffer, int valueOffset, int valueLength);

	protected void copy_to(long buffer) throws CastleException
	{
		int keyLength = key.copyToBuffer(keyBuffer);
		copy_to(buffer, collectionId, keyBuffer, keyBuffer.position(), keyLength, valueBuffer, valueBuffer.position(),
				valueBuffer.remaining());
	}
}
