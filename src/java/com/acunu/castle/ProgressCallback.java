package com.acunu.castle;

import java.util.concurrent.atomic.AtomicInteger;

public class ProgressCallback
{
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
		return callback == null ? null : new Callback()
		{
			@Override
			protected void call(RequestResponse response)
			{
				if (0 >= remaining.addAndGet(-taskProgress))
					ProgressCallback.this.call(response);
			}
			@Override
			protected void handleError(int error)
			{
				firstError.compareAndSet(0, error);
				if (0 >= remaining.addAndGet(-taskProgress))
					ProgressCallback.this.call(response);
			}
		};
	}
}
