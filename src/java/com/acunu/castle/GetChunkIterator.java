package com.acunu.castle;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class GetChunkIterator implements Iterator<ByteBuffer>, Closeable
{
	private final Castle castle;
	private final long token;
	private final long length;
	
	private ByteBuffer buffer;
	private long read = 0;
	
	public GetChunkIterator(final Castle castle, final int collection, final Key key) throws IOException
	{
		this(castle, collection, key, 1 << 20);
	}
	
	public GetChunkIterator(final Castle castle, final int collection, final Key key, final int chunkSize) throws IOException
	{
		if (chunkSize > Castle.MAX_BUFFER_SIZE)
			throw new CastleException(-1, "Invalid chunk size");
		
		this.castle = castle;
		buffer = castle.getBuffer(chunkSize);
		
		final BigGetReply reply = castle.big_get(collection, key);
		if (!reply.found)
			throw new CastleException(-2, "Key not found");
		
		this.token = reply.token;
		this.length = reply.length;
	}
	
	@Override
	public boolean hasNext()
	{
		return read < length; 
	}

	@Override
	public ByteBuffer next()
	{
		buffer.rewind();
		try
		{
			castle.get_chunk(token, buffer);
		} catch (IOException e)
		{
			throw new RuntimeException(e);
		}
		read += buffer.remaining();
		return buffer;
	}

	@Override
	public void remove()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() throws IOException
	{
		castle.putBuffer(buffer);
	}
}
