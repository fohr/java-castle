package com.acunu.castle;

import java.nio.ByteBuffer;

/**
 * Starts a new iterator over ordered key-value pairs in the hypercube specified
 * by a start and end key.
 */
public final class IterStartRequest extends Request
{
	public final Key startKey;
	public final Key endKey;
	public final int collectionId;

	public final ByteBuffer startKeyBuffer;
	public final ByteBuffer endKeyBuffer;
	public final IterFlags flags;

	public enum IterFlags
	{
		NONE(0x0),      /* CASTLE_RING_FLAG_NONE            */
		NO_VALUES(0x3); /* CASTLE_RING_FLAG_ITER_NO_VALUES  */
		
		public long val;
		
		private IterFlags(long val)
		{
			this.val = val;
		}
	}

	public IterStartRequest(Key startKey, Key endKey, int collectionId, ByteBuffer startKeyBuffer,
			ByteBuffer endKeyBuffer, IterFlags flags)
	{
		super(CASTLE_RING_ITER_START);

		this.startKey = startKey;
		this.endKey = endKey;
		this.collectionId = collectionId;

		/*
		 * Take a slice so the caller can continue to change position/limit in
		 * the original buffer.
		 */
		this.startKeyBuffer = startKeyBuffer.slice();
		this.endKeyBuffer = endKeyBuffer.slice();

		this.flags = flags;
	}

	static private native void copy_to(long buffer, int index, int collectionId, ByteBuffer startKeyBuffer, int startKeyOffset,
			int startKeyLength, ByteBuffer endKeyBuffer, int endKeyOffset, int endKeyLength, long flags);

	@Override
	protected void copy_to(long buffer, int index) throws CastleException
	{
		int startKeyLength = startKey.copyToBuffer(startKeyBuffer);
		int endKeyLength = endKey.copyToBuffer(endKeyBuffer);
		copy_to(buffer, index, collectionId, startKeyBuffer, startKeyBuffer.position(), startKeyLength, endKeyBuffer,
				endKeyBuffer.position(), endKeyLength, flags.val);
	}
}
