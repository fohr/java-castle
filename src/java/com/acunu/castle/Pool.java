package com.acunu.castle;

import java.util.concurrent.ConcurrentLinkedQueue;

public class Pool<T>
{
	private ConcurrentLinkedQueue<T> objects;
	private Factory<T> fact;
	
	public static interface Factory<E>
	{
		E create(Pool<E> pool);
		void destroy(E obj);
	}
	
	public Pool(int size, Factory<T> fact)
	{
		this.fact = fact;
		objects = new ConcurrentLinkedQueue<T>();
		for (int i = 0; i < size; ++i)
			objects.offer(fact.create(this));
	}
	
	public void destroy()
	{
		T obj;
		while (null != (obj = objects.poll()))
			fact.destroy(obj);
	}
	
	public T tryLease()
	{
		return objects.poll();
	}

	public T lease() throws InterruptedException
	{
		T obj = objects.poll();
		if (obj != null)
			return obj;
		synchronized (objects)
		{
			while (null == (obj = objects.poll()))
				objects.wait();
		}
		return obj;
	}

	public void release(T obj)
	{
		objects.offer(obj);
		synchronized (objects)
		{
			objects.notify();
		}
	}
}
