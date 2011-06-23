package com.acunu.castle;

import java.util.concurrent.atomic.AtomicInteger;


public abstract class MultiCallback extends AbstractCallback
{
	private final AtomicInteger received = new AtomicInteger(0);
	private int numRequests;
	private RequestResponse[] responses;
	private int[] errors;

	class SingleCallback extends Callback
	{
		final int index;
		
		SingleCallback(int index)
		{
			this.index = index;
		}
		
		@Override
		protected void call(RequestResponse response)
		{
			responses[index] = response;
			errors[index] = 0;
			if (received.incrementAndGet() == numRequests)
				MultiCallback.this.run();
		}

		@Override
		protected void handleError(int error)
		{	
			responses[index] = response;
			errors[index] = error;
			if (received.incrementAndGet() == numRequests)
				MultiCallback.this.run();
		}
	}
	
	Callback[] getCallbacks(int numRequests)
	{
		this.numRequests = numRequests;
		this.responses = new RequestResponse[numRequests];
		this.errors = new int[numRequests];
		
		Callback[] ret = new Callback[numRequests];
		for (int i = 0; i < numRequests; ++i)
			ret[i] = new SingleCallback(i);
		return ret;
	}
	
	protected abstract void call(RequestResponse[] response, int[] errors);

	@Override
	protected void process()
	{
		call(responses, errors);
	}

}
