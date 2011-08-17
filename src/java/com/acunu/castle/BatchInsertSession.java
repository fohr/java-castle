package com.acunu.castle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * put_multi with zero-copying for values
 * NB: single-threaded, non-reentrant use only! 
 */
public class BatchInsertSession
{
	private List<Request> requests;
	private ByteBuffer keyBuf;
	private ByteBuffer currentBuf;
	private final Castle castle;
	private final int collection;
	private final boolean async;
	private AtomicInteger remaining = new AtomicInteger(0);
	private CastleException error = null;
	private boolean closed = false;
	
	public BatchInsertSession(final Castle castle, final int collection, final boolean async) throws IOException
	{
		this.castle = castle;
		this.collection = collection;
		this.async = async;

		ByteBuffer[] buffers = castle.getBuffers(Castle.MAX_BUFFER_SIZE, Castle.MAX_BUFFER_SIZE);
		keyBuf = buffers[0];
		currentBuf = buffers[1];
		requests = new ArrayList<Request>();
	}
	
	/**
	 * @return number of requests pending insertion in the current batch
	 */
	public int size()
	{
		return requests == null ? 0 : requests.size();
	}

	/**
	 * Start the batch insert session
	 * @return the buffer in which to store the first value
	 */
	public ByteBuffer start()
	{
		requests.clear();
		currentBuf.rewind();
		keyBuf.rewind();
		return currentBuf.slice();
	}

	/**
	 * Flush current batch and free buffers used for keys/values. In async mode, this method
	 * will block until all batches have finished being inserted.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void close() throws IOException, InterruptedException
	{
		if (!closed)
		{
			flush();
			if (async)
			{
				synchronized (remaining)
				{
					while (remaining.get() > 0)
						remaining.wait();
				}
			}
			castle.putBuffers(keyBuf, currentBuf);
			closed = true;
			if (error != null)
				throw error;
		}
	}

	/**
	 * insert the current batch 
	 * @return the buffer for the next value to be written to
	 * @throws IOException
	 */
	public ByteBuffer flush() throws IOException
	{
		if (!requests.isEmpty())
		{
			if (async)
			{
				remaining.incrementAndGet();
				Callback cb = new Callback()
				{
					@Override
					protected void call(RequestResponse response)
					{
						synchronized (remaining)
						{
							remaining.decrementAndGet();
							remaining.notify();
						}
					}
	
					@Override
					protected void handleError(int error)
					{
						BatchInsertSession.this.error = new CastleException(error, "Error during insert");
						synchronized (remaining)
						{
							remaining.decrementAndGet();
							remaining.notify();
						}
					}
				};
				cb.collect(castle, currentBuf, keyBuf);
				castle.castle_request_send_multi_ex(requests.toArray(new Request[requests.size()]), cb);
				ByteBuffer[] buffers = castle.getBuffers(Castle.MAX_BUFFER_SIZE, Castle.MAX_BUFFER_SIZE);
				keyBuf = buffers[0];
				currentBuf = buffers[1];
			} else
				castle.castle_request_blocking_multi(requests.toArray(new Request[requests.size()]));
		}
		return start();
	}
	
	/**
	 * Add this key and value to the current batch. If the buffers are full, the batch will be inserted
	 * and this key/value will be added to a new batch.
	 * @param key
	 * @param value
	 * @return the buffer for the next value to be written to. The caller should check buffer.remaining()
	 * to ensure it is large enough for the next value, and if not, call {@link flush()}
	 * @throws IOException if insertion fails
	 */
	public ByteBuffer put(final Key key, final ByteBuffer value) throws IOException
	{
		if (Castle.MAX_KEY_SIZE > keyBuf.remaining())
			flush();
		assert currentBuf.remaining() >= value.remaining();
		currentBuf.position(currentBuf.position() + value.remaining());
		
		ByteBuffer curKey = keyBuf.slice();
		curKey.limit(Castle.MAX_KEY_SIZE);
		keyBuf.position(keyBuf.position() + Castle.MAX_KEY_SIZE);
		
		requests.add(new ReplaceRequest(key, collection, curKey, value));
		return currentBuf.slice();
	}
}
