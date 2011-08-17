package com.acunu.castle;

/**
 * Closes the iterator with the given token. Should be called whenever an
 * iterator is finished with.
 */
public final class IterFinishRequest extends Request
{
	public final long token;

	public IterFinishRequest(long token)
	{
		super(CASTLE_RING_ITER_FINISH);

		this.token = token;
	}

	static private native void copy_to(long buffer, int index, long token);

	@Override
	protected void copy_to(long buffer, int index) throws CastleException
	{
		copy_to(buffer, index, token);
	}
}
