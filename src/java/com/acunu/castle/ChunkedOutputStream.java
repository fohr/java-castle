package com.acunu.castle;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ChunkedOutputStream extends OutputStream
{
	private final Castle castle;
	private final int collection;
	private final Key key;
	private final long chunkSize;
	
	private ByteBuffer curBuf;
	private List<ByteBuffer> chunkBufs = new ArrayList<ByteBuffer>();
	private long chunkNum = 0;
	private long bytesWritten = 0;
	private final ThreadPoolExecutor executor;
	
	private IOException error = null;
	
	public ChunkedOutputStream(final Castle castle, final int collection, final Key key) throws IOException
	{
		this(castle, collection, key, 1 << 26 /* 64Mb */, 1);
	}
	
	public ChunkedOutputStream(final Castle castle, final int collection, final Key key, final long chunkSize) throws IOException
	{
		this(castle, collection, key, chunkSize, 1);
	}
	
	public ChunkedOutputStream(final Castle castle, final int collection, final Key key, final long chunkSize,
			final int bufferFactor) throws IOException
	{
		if (bufferFactor < 1)
			throw new IllegalArgumentException("bufferFactor < 1");
		
		this.castle = castle;
		this.collection = collection;
		this.chunkSize = chunkSize;
		this.key = key;
		executor = new ThreadPoolExecutor(1, 1, Long.MAX_VALUE, TimeUnit.NANOSECONDS,
				new LinkedBlockingQueue<Runnable>(bufferFactor));
		nextBuf();
	}
	
	private void nextBuf() throws IOException
	{
		if (curBuf != null)
			curBuf.flip();
		curBuf = castle.getBuffer(1 << 20);
		chunkBufs.add(curBuf);
	}
	
	void put() throws IOException
	{
		if (chunkBufs.size() == 1 && curBuf.position() == 0)
			return;
		
		final ByteBuffer[] bufs = chunkBufs.toArray(new ByteBuffer[chunkBufs.size()]);
		chunkBufs = new ArrayList<ByteBuffer>();
		nextBuf();
		final long len = bytesWritten;
		bytesWritten = 0;
		
		final long curChunkNum = chunkNum++;
		executor.execute(new Runnable()
		{
			public void run()
			{
				try
				{
					final Key chunkKey = key.extend(ByteBuffer.allocate(Long.SIZE/8).putLong(curChunkNum).array());
					BigPutReply token = castle.big_put(collection, chunkKey, len);
					castle.put_chunks(token.token, bufs);
				} catch (IOException e)
				{
					error = e;
				} finally
				{
					try
					{
						castle.putBuffers(bufs);
					} catch (IOException e)
					{
						error = e;
					}
				}
			}
		});
	}
	
	private void checkError() throws IOException
	{
		if (error != null)
			throw error;
	}
	
	@Override
	public void close() throws IOException
	{
		flush();
		executor.shutdown();
		try
		{
			executor.awaitTermination(365, TimeUnit.DAYS);
		} catch (InterruptedException e)
		{
			throw new IOException(e);
		}
		
		checkError();
	}

	@Override
	public void flush() throws IOException
	{
		checkError();
		put();
	}

	@Override
	public void write(int b) throws IOException
	{
		write(new byte[]{(byte) (b & 0xFF)});
	}

	@Override
	public void write(byte[] b) throws IOException
	{
		write(b, 0, b.length);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException
	{
		checkError();
		
		if (bytesWritten + len > chunkSize)
			put();
		
		int remaining = curBuf.limit() - curBuf.position();
		if (len > remaining)
			nextBuf();
		
		curBuf.put(b, off, len);
		bytesWritten += len;
	}
}
