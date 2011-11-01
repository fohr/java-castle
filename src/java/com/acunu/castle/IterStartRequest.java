package com.acunu.castle;

import java.nio.ByteBuffer;
import java.util.EnumSet;

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
        public final ByteBuffer buffer;
	public final EnumSet<IterFlags> flags;

	public enum IterFlags
	{
		NONE(1 << 0),         /* CASTLE_RING_FLAG_NONE            */
		NO_PREFETCH(1 << 1),  /* CASTLE_RING_FLAG_NO_PREFETCH     */
		NO_CACHE(1 << 2),     /* CASTLE_RING_FLAG_NO_CACHE        */
		NO_VALUES(1 << 3),    /* CASTLE_RING_FLAG_ITER_NO_VALUES  */
		GET_OOL(1 << 4);      /* CASTLE_RING_FLAG_ITER_GET_OOL    */

		public long val;

		private IterFlags(long val)
		{
			this.val = val;
		}
	}

	public IterStartRequest(Key startKey, Key endKey, int collectionId, ByteBuffer startKeyBuffer,
			ByteBuffer endKeyBuffer, ByteBuffer buffer, EnumSet<IterFlags> flags)
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
		this.buffer = buffer.slice();
		this.flags = flags;
	}

	static private native void copy_to(long buffer, int index, int collectionId, ByteBuffer startKeyBuffer, int startKeyOffset,
			int startKeyLength, ByteBuffer endKeyBuffer, int endKeyOffset, int endKeyLength, ByteBuffer bbuffer,
                        int bufferOffset, int bufferLength, long flags);

	@Override
	protected void copy_to(long target_buffer, int index) throws CastleException
	{
		long flagValue = 0;
		for (IterFlags flag : flags)
			flagValue |= flag.val;

		int startKeyLength = startKey.copyToBuffer(startKeyBuffer);
		int endKeyLength = endKey.copyToBuffer(endKeyBuffer);
		copy_to(target_buffer, index, collectionId, startKeyBuffer, startKeyBuffer.position(), startKeyLength, endKeyBuffer,
				endKeyBuffer.position(), endKeyLength, buffer, buffer.position(), buffer.remaining(), flagValue);
	}
}
