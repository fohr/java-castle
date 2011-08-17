package com.acunu.castle;

import java.util.concurrent.atomic.AtomicInteger;

public class ProgressCallback
{
	private static final Pool<TaskCallback> cbPool = new Pool<TaskCallback>(1024, new Pool.Factory<TaskCallback>()
	{
		@Override
		public TaskCallback create(Pool pool)
		{
			return new TaskCallback(pool);
		}

		@Override
		public void destroy(TaskCallback obj)
		{
		}
	});
	
	private final Callback callback;
	private final AtomicInteger remaining;
	private final AtomicInteger firstError;
	
	public ProgressCallback(Callback callback, int totalProgress)
	{
		this.callback = callback;
		this.remaining = new AtomicInteger(totalProgress);
		this.firstError = new AtomicInteger(0);
	}
	
	private void call(RequestResponse response)
	{
		callback.setResponse(response);
		callback.setErr(firstError.get());
		callback.run();
	}
	
	public Callback getCallback(final int taskProgress)
	{
		if (callback == null)
			return null;
		TaskCallback cb;
		try
		{
			cb = cbPool.lease();
		} catch (InterruptedException e)
		{
			throw new RuntimeException(e);
		}
		cb.setParams(this, remaining, firstError, taskProgress);
		return cb;
	}
	
	private static class TaskCallback extends PooledCallback
	{
		private ProgressCallback callback;
		private AtomicInteger remaining;
		private AtomicInteger firstError;
		private int taskProgress;
		
		public TaskCallback(Pool<PooledCallback> pool)
		{
			super(pool);
		}
		
		void setParams(final ProgressCallback callback, final AtomicInteger remaining, final AtomicInteger firstError,
				int taskProgress)
		{
			this.callback = callback;
			this.remaining = remaining;
			this.firstError = firstError;
			this.taskProgress = taskProgress;
		}
		
		@Override
		protected void call(RequestResponse response)
		{
			if (0 >= remaining.addAndGet(-taskProgress))
				callback.call(response);
		}
		@Override
		protected void handleError(int error)
		{
			firstError.compareAndSet(0, error);
			if (0 >= remaining.addAndGet(-taskProgress))
				callback.call(response);
		}
	}
}
