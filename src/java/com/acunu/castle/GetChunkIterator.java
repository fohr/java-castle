package com.acunu.castle;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An {@link Iterator} implementation for fetching chunks of a large value. Buffers are returned in 1Mb blocks,
 * but a chunk of <code>chunkSize</code> is fetched at once. While one chunk is being returned from next(), the next
 * chunk is prefetched. The memory usage is therefore 2 * <code>chunkSize</code>.
 * <br><b>NB: buffers returned from next() must be freed with {@link Castle#putBuffer(ByteBuffer)}
 */
public class GetChunkIterator implements Iterator<ByteBuffer>, Closeable
{
	private final Castle castle;
	private final long token;
	private final long length;
	
	private List<AtomicReference<ByteBuffer>> curChunk;
	private List<AtomicReference<ByteBuffer>> nextChunk;
	private int curBuf;
	private final int bufferSize;
	private final int buffersPerChunk;
	private long fetched = 0;
	private long read = 0;
	
	private Exception lastErr = null;
	
	public GetChunkIterator(final Castle castle, final int collection, final Key key) throws IOException
	{
		this(castle, collection, key, 1 << 20);
	}
	
	public GetChunkIterator(final Castle castle, final int collection, final Key key, final int chunkSize) throws IOException
	{
		bufferSize = Math.min(chunkSize, Castle.MAX_BUFFER_SIZE);
		buffersPerChunk = (chunkSize + bufferSize - 1) / bufferSize;
		
		curChunk = new ArrayList<AtomicReference<ByteBuffer>>(buffersPerChunk);
		for (int i = 0; i < buffersPerChunk; ++i)
			curChunk.add(new AtomicReference<ByteBuffer>());
		
		nextChunk = new ArrayList<AtomicReference<ByteBuffer>>(buffersPerChunk);
		for (int i = 0; i < buffersPerChunk; ++i)
			nextChunk.add(new AtomicReference<ByteBuffer>());
		
		this.curBuf = 0;
		
		this.castle = castle;
		
		final BigGetReply reply = castle.big_get(collection, key);
		if (!reply.found)
			throw new CastleException(-2, "Key not found");
		
		this.token = reply.token;
		this.length = reply.length;
		
		nextChunk();
		nextChunk();
	}
	
	private void nextChunk() throws IOException
	{
		/* swap pre-fetched chunk to current chunk */
		final List<AtomicReference<ByteBuffer>> tmp = curChunk;
		curChunk = nextChunk;
		nextChunk = tmp;
		curBuf = 0;
		
		/* pre-fetch next chunk */
		if (fetched < length)
		{
			final Integer[] sizes = new Integer[buffersPerChunk];
			Arrays.fill(sizes, bufferSize);
			final ByteBuffer[] bufs = castle.getBuffers(sizes);
			for (int i = 0; i < buffersPerChunk; ++i)
			{
				final ByteBuffer buf = bufs[i];
				if (fetched >= length)
				{
					castle.putBuffer(buf);
					continue;
				}
				final AtomicReference<ByteBuffer> pBuf = nextChunk.get(i);
				castle.get_chunk(token, buf, new Callback()
				{
					@Override
					protected void call(RequestResponse response)
					{
						synchronized(pBuf)
						{
							pBuf.set(buf);
							pBuf.notify();
						}
					}
					@Override
					protected void handleError(int error)
					{
						try
						{
							castle.putBuffer(buf);
						} catch (IOException e)
						{
						}
						lastErr = new CastleException(error, "Error during get_chunk");
					}
				});
				fetched += bufferSize;
			}
		}
	}

	@Override
	public boolean hasNext()
	{
		return read < length;
	}
	
	private void checkError()
	{
		if (lastErr != null)
			throw new RuntimeException(lastErr);
	}

	@Override
	public ByteBuffer next()
	{
		checkError();
		try
		{
			final AtomicReference<ByteBuffer> next = curChunk.get(curBuf++);
			ByteBuffer nextBuf;
			synchronized(next)
			{
				while(next.get() == null)
						next.wait();
				nextBuf = next.getAndSet(null);
			}
			if (curBuf == buffersPerChunk)
				nextChunk();
			
			read += nextBuf.remaining();

			return nextBuf;
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public void remove()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() throws IOException
	{
		for (AtomicReference<ByteBuffer> buf : curChunk)
			if (buf.get() != null)
				castle.putBuffer(buf.get());
		for (AtomicReference<ByteBuffer> buf : nextChunk)
			if (buf.get() != null)
				castle.putBuffer(buf.get());
	}
}
