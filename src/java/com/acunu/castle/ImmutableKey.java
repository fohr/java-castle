package com.acunu.castle;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.acunu.castle.Key.KeyDimensionFlags;

public class ImmutableKey
{
	public final ByteBuffer[] key;
	public final KeyDimensionFlags[] flags;
	
	public ImmutableKey(final ByteBuffer byteBuffer)
	{
		ByteBuffer packedKey = byteBuffer.slice();
		int length = packedKey.order(ByteOrder.LITTLE_ENDIAN).getInt();
		assert length == packedKey.remaining();
		
		int numDimensions = packedKey.getInt();
		packedKey.getInt(); /* unused */
		
		this.key = new ByteBuffer[numDimensions];
		this.flags = new KeyDimensionFlags[numDimensions];
		int[] offsets = new int[numDimensions];
		for (int i = 0; i < numDimensions; ++i)
		{
			int hdr = packedKey.getInt();
			offsets[i] = hdr >> 8;
			flags[i] = KeyDimensionFlags.valueOf((byte)(hdr & 0xFF));
		}
		
		for (int i = 0; i < numDimensions; ++i)
		{
			ByteBuffer dim = byteBuffer.slice();
			dim.position(offsets[i]);
			dim.limit(i == numDimensions - 1 ? length : offsets[i+1]);
			key[i] = dim.slice();
		}
	}
	
	// TODO: remove the need for this by making Key use ByteBuffers 
	public Key mutable()
	{
		final byte[][] dims = new byte[key.length][];
		int i = 0;
		for (ByteBuffer dim : key)
		{
			ByteBuffer tmp = dim.slice();
			dims[i] = new byte[tmp.remaining()];
			tmp.get(dims[i++]);
		}
		return new Key(dims);
	}
}
