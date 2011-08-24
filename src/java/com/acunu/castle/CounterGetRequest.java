package com.acunu.castle;

import java.nio.ByteBuffer;

public class CounterGetRequest extends GetRequest
{
	public CounterGetRequest(Key key, int collectionId, ByteBuffer keyBuffer, ByteBuffer valueBuffer)
	{
		super(key, collectionId, keyBuffer, valueBuffer);
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
