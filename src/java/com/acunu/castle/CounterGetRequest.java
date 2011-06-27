package com.acunu.castle;

import java.nio.ByteBuffer;

public class CounterGetRequest extends GetRequest
{
	public CounterGetRequest(Key key, int collectionId, ByteBuffer keyBuffer, ByteBuffer valueBuffer)
	{
		super(key, collectionId, keyBuffer, valueBuffer);
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
