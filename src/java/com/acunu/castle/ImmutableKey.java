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

		// the total length of the serialized key in the byte buffer. Note the byte buffer
		// could contain extra data afterwards (so serializedLength < byteBuffer.remaining())
		// because in iterator buffers, there are more keys, etc..
		int length = packedKey.order(ByteOrder.LITTLE_ENDIAN).getInt();

		packedKey.limit(packedKey.position() + length);

		int numDimensions = packedKey.getInt();
		packedKey.getLong(); /* unused */

		this.key = new ByteBuffer[numDimensions];
		this.flags = new KeyDimensionFlags[numDimensions];
		int[] offsets = new int[numDimensions];
		for (int i = 0; i < numDimensions; ++i)
		{
			int hdr = packedKey.getInt();
			offsets[i] = hdr >> 8;
			flags[i] = KeyDimensionFlags.valueOf((byte) (hdr & 0xFF));
		}

		for (int i = 0; i < numDimensions; ++i)
		{
			ByteBuffer dim = byteBuffer.slice();
			dim.position(offsets[i]);
			dim.limit(i == numDimensions - 1 ? packedKey.limit() : offsets[i + 1]);
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
			if (tmp.remaining() > 512)
				System.out.println("tmp.remaining() = " + tmp.remaining());
			dims[i] = new byte[tmp.remaining()];
			tmp.get(dims[i++]);
		}
		return new Key(dims);
	}
}
