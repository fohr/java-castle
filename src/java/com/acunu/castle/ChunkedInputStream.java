package com.acunu.castle;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.EnumSet;

import com.acunu.castle.IterStartRequest.IterFlags;

/**
 * {@link InputStream} implementation to read data written by {@link ChunkedOutputStream}.
 * Memory usage is equal to <code>bufferSize</code>.
 */
public class ChunkedInputStream extends InputStream
{
	private final Castle castle;
	private final int collection;
	private final int bufferSize;
	
	private final LargeKeyValueIterator keyIter;
	private GetChunkIterator valueIter;
	private ByteBuffer current;

	public ChunkedInputStream(final Castle castle, final int collection, final Key key) throws IOException
	{
		this(castle, collection, key, Castle.MAX_BUFFER_SIZE);
	}
	
	public ChunkedInputStream(final Castle castle, final int collection, final Key key, final int bufferSize) throws IOException
	{
		this.castle = castle;
		this.collection = collection;
		this.bufferSize = bufferSize;
		final Key startKey = key.extend(new byte[0]);
		keyIter = new LargeKeyValueIterator(castle, collection, startKey, startKey, 1 << 20, 0, 10, EnumSet.of(IterFlags.NONE), null);
		nextBuf();
	}

	private void nextBuf() throws IOException
	{
		if (current != null)
		{
			castle.putBuffer(current);
			current = null;
		}
		if (valueIter == null || !valueIter.hasNext())
		{
			if (!keyIter.hasNext())
				return;
			if (valueIter != null)
				valueIter.close();
			valueIter = new GetChunkIterator(castle, collection, keyIter.next().getKey(), bufferSize);
		}
		
		current = valueIter.next();
	}

	@Override
	public int available() throws IOException
	{
		return current == null ? 0 : current.remaining();
	}

	@Override
	public void close() throws IOException
	{
		if (valueIter != null)
			valueIter.close();
		keyIter.close();
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException
	{
		if (current != null && current.remaining() == 0)
			nextBuf();
		
		if (current == null)
			return -1;
		
		int read = Math.min(len, current.remaining());
		current.get(b, off, read);
		return read;
	}

	@Override
	public int read(byte[] b) throws IOException
	{
		return read(b, 0, b.length);
	}

	@Override
	public int read() throws IOException
	{
		byte[] b = new byte[1];
		int read = read(b);
		if (read < 1)
			return -1;
		return b[0] & 0xFF;
	}
}
