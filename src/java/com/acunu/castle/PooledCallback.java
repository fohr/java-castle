package com.acunu.castle;

public abstract class PooledCallback extends Callback
{
	private Pool<PooledCallback> pool;

	public PooledCallback(Pool<PooledCallback> pool)
	{
		this.pool = pool;
	}

	@Override
	public void cleanup()
	{
		super.cleanup();
		setResponse(null);
		setErr(0);
		pool.release(this);
	}
}
