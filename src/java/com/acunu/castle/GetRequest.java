package com.acunu.castle;

import java.nio.ByteBuffer;

/**
 * A request to get any value associated with a single key. Will only return
 * values <= 1 MB; for larger values, use a big_get.
 */
public class GetRequest extends Request
{
	public final int collectionId;

	public final ByteBuffer keyBuffer;
	public final ByteBuffer valueBuffer;
	public final int keyLen;

	public GetRequest(Key key, int collectionId, ByteBuffer keyBuffer, ByteBuffer valueBuffer)
	{
		this(collectionId, keyBuffer, copyKey(key, keyBuffer), valueBuffer);
	}
	
	public GetRequest(int collectionId, ByteBuffer keyBuffer, int keyLen, ByteBuffer valueBuffer)
	{
		super(CASTLE_RING_GET);

		this.collectionId = collectionId;
		this.keyLen = keyLen;
		/*
		 * Take a slice so the caller can continue to change position/limit in
		 * the original buffer.
		 */
		this.keyBuffer = keyBuffer.slice();
		this.valueBuffer = valueBuffer.slice();
	}

	static private native void copy_to(long buffer, int index, int collectionId, ByteBuffer keyBuffer, int keyOffset,
			int keyLength, ByteBuffer valueBuffer, int valueOffset, int valueLength);

	@Override
	protected void copy_to(long buffer, int index) throws CastleException
	{
		copy_to(buffer, index, collectionId, keyBuffer, keyBuffer.position(), keyLen, valueBuffer, valueBuffer.position(),
				valueBuffer.remaining());
	}
}
