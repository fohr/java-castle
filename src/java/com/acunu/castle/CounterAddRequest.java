package com.acunu.castle;

import java.nio.ByteBuffer;

public class CounterAddRequest extends Request
{
	protected final Key key;
	protected final int collection;
	protected final ByteBuffer keyBuffer;
	protected final ByteBuffer valueBuffer;

	protected CounterAddRequest(final Key key, final int collection, final ByteBuffer keyBuffer, final ByteBuffer valueBuffer)
	{
		super(CASTLE_RING_COUNTER_ADD_REPLACE);
		this.key = key;
		this.collection = collection;
		this.keyBuffer = keyBuffer;
		this.valueBuffer = valueBuffer;
	}

	static private native void copy_to(long buffer, int index, int collectionId, ByteBuffer keyBuffer, int keyOffset,
			int keyLength, ByteBuffer valueBuffer, int valueOffset, int valueLength);

	@Override
	protected void copy_to(long buffer, int index) throws CastleException
	{
		int keyLength = key.copyToBuffer(keyBuffer);
		copy_to(buffer, index, collection, keyBuffer, keyBuffer.position(), keyLength, valueBuffer, valueBuffer.position(),
				valueBuffer.remaining());
	}
}
