package com.acunu.castle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

public class BufferManager
{
	private final Castle castle;
	private boolean closed;

	private final Map<Integer, LinkedList<ByteBuffer>> buffers;
	private final Map<Integer, Integer> sizeCountMap;
	private final List<Integer> sizes;
	private final boolean blocking;
	
	BufferManager(final Castle castle) throws IOException
	{
		this(castle, new HashMap<Integer, Integer>());
	}

	BufferManager(final Castle castle, final Map<Integer, Integer> sizeCountMap) throws IOException
	{
		this(castle, sizeCountMap, false);
	}

	BufferManager(final Castle castle, final Map<Integer, Integer> sizeCountMap, final boolean blocking)
	        throws IOException
	{
		this.castle = castle;
		this.sizeCountMap = sizeCountMap;
		this.blocking = blocking;

		buffers = new HashMap<Integer, LinkedList<ByteBuffer>>(sizeCountMap.size());
		sizes = new ArrayList<Integer>(sizeCountMap.size());

		for (final Entry<Integer, Integer> entry : sizeCountMap.entrySet())
		{
			final LinkedList<ByteBuffer> bufferList = new LinkedList<ByteBuffer>();

			final int size = entry.getKey();

			for (int i = 0; i < entry.getValue(); i++)
			{
				bufferList.add(castle.createBuffer(size));
			}

			buffers.put(size, bufferList);
			sizes.add(size);
		}

		Collections.sort(sizes);
	}

	@Override
	protected void finalize() throws IOException
	{
		close();
	}

	void close() throws IOException
	{
		if (closed)
			return;

		IOException exn = null;
		for (final Entry<Integer, LinkedList<ByteBuffer>> entry : buffers.entrySet())
		{
			for (final ByteBuffer buffer : entry.getValue())
			{
				try
				{
					castle.destroyBuffer(buffer);
				} catch (final IOException e)
				{
					exn = e;
				}
			}
		}
		closed = true;
		if (exn != null)
			throw exn;
	}
	
	public ByteBuffer get(final int size) throws IOException
	{
		if (closed)
			throw new IOException("BufferManager is closed");
		
		if (size == 0)
			return ByteBuffer.allocateDirect(0);
		
		final Integer sizeToUse = getAllocatedSize(size);
		if (sizeToUse == null)
			return castle.createBuffer(size);
		
		final List<ByteBuffer> buffers = lease(sizeToUse, 1);
		if (buffers.isEmpty())
			return castle.createBuffer(size);
		
		buffers.get(0).limit(size);
		return buffers.get(0);
	}
	
	/*
	 * leases one buffer of each of the supplied sizes in such a way as to avoid deadlock.
	 * Deadlock-safety is only implied if this thread has not already leased any other buffers.
	 */
	public ByteBuffer[] get(final Integer... sizes) throws IOException
	{
		if (closed)
			throw new IOException("BufferManager is closed");
		
		final Map<Integer, Integer> requestedSizes = new HashMap<Integer, Integer>();
		for (final Integer size : sizes)
		{
			if (requestedSizes.containsKey(size))
				requestedSizes.put(size, 1 + requestedSizes.get(size));
			else
				requestedSizes.put(size, 1);
		}
		
		final List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(sizes.length);
		final Map<Integer, List<ByteBuffer>> allocatedBuffers = lease(requestedSizes);
		
		boolean failed = false;
		try
		{
			// TODO: do this in reverse order of size so that any allocations are of smaller size
			for (final Integer size : sizes)
			{
				if (allocatedBuffers.get(size).isEmpty())
					buffers.add(castle.createBuffer(size));
				else
				{
					ByteBuffer buf = allocatedBuffers.get(size).remove(0);
					buf.limit(size);
					buffers.add(buf);
				}
			}
			
			return buffers.toArray(new ByteBuffer[buffers.size()]);
		} catch(IOException e)
		{
			failed = true;
			throw e;
		} catch(RuntimeException e)
		{
			failed = true;
			throw e;
		} finally 
		{
			if (failed)
			{
				for (List<ByteBuffer> bufs : allocatedBuffers.values())
				{
					put(bufs.toArray(new ByteBuffer[bufs.size()]));
					bufs.clear();
				}
				put(buffers.toArray(new ByteBuffer[buffers.size()]));
			}
		}
	}
	
	/*
	 * returns the smallest allocated buffer size >= requestedSize
	 * returns null if no allocated buffer is large enough, or if requestedSize == 0
	 */
	private Integer getAllocatedSize(final int requestedSize)
	{
		if (requestedSize > Castle.MAX_BUFFER_SIZE)
			throw new IllegalArgumentException("Buffer size requested greater than Castle.MAX_BUFFER_SIZE");
		
		if (requestedSize == 0)
			return null;
		
		// find the next largest size we have
		Integer sizeToUse = null;
		for (final int availableSize : sizes)
		{
			if (availableSize >= requestedSize)
			{
				sizeToUse = availableSize;
				break;
			}
		}
		return sizeToUse;
	}

	/*
	 * leases N buffers of size S as specified by sizes
	 * returns a Map of size S to list of buffers
	 * A list may be reused between sizes if more than one size mapped to the same allocated buffer size
	 * In blocking mode will always return a List of size N1 + N2 if both size S1 and S2 mapped to the same
	 *   allocated buffer size (both S1 and S2 map to the same list)
	 * In non-blocking mode, the List size may be less than N1 + N2 if not enough buffers were available.
	 */
	private Map<Integer, List<ByteBuffer>> lease(final Map<Integer, Integer> sizes) throws IOException
	{
		Map<Integer, List<ByteBuffer>> result = new HashMap<Integer, List<ByteBuffer>>();
		Map<Integer, List<ByteBuffer>> allocated = new HashMap<Integer, List<ByteBuffer>>();
		boolean failed = false;
		try
		{
			final SortedMap<Integer, Integer> sizesToUse = new TreeMap<Integer, Integer>();
			for (Integer size : sizes.keySet())
			{
				if (size == 0)
				{
					ByteBuffer[] emptyBufs = new ByteBuffer[sizes.get(size)];
					Arrays.fill(emptyBufs, ByteBuffer.allocateDirect(0));
					result.put(size, new ArrayList<ByteBuffer>(Arrays.asList(emptyBufs)));
					continue;
				}
					
				final Integer sizeToUse = getAllocatedSize(size);
				if (sizeToUse == null)
				{
					result.put(size, new ArrayList<ByteBuffer>(sizes.get(size)));
					for (int i = 0; i < sizes.get(size); ++i)
						result.get(size).add(castle.createBuffer(size));
					continue;
				}
				
				if (!sizesToUse.containsKey(sizeToUse))
				{
					sizesToUse.put(sizeToUse, sizes.get(size));
					allocated.put(sizeToUse, new ArrayList<ByteBuffer>());
				}
				else
					sizesToUse.put(sizeToUse, sizesToUse.get(sizeToUse) + sizes.get(size));
				
				result.put(size, allocated.get(sizeToUse));
			}
			
			/* lease in ascending size order to avoid deadlock */
			for (final Integer size : sizesToUse.keySet())
			{
				allocated.get(size).addAll(lease(size, sizesToUse.get(size)));
			}
		} catch(IOException e)
		{
			failed = true;
			throw e;
		} catch(RuntimeException e)
		{
			failed = true;
			throw e;
		} finally 
		{
			if (failed)
			{
				for (final List<ByteBuffer> bufs : result.values())
				{
					put(bufs.toArray(new ByteBuffer[bufs.size()]));
					bufs.clear();
				}
			}
		}
		
		return result;
	}	
	
	/*
	 * Gets num buffers of the exact size specified. 
	 * In blocking mode, will always return exactly num buffers of requested size.
	 * In non-blocking mode, may return fewer than requested if not enough were available.
	 * It is an error to request a size which was not allocated.
	 */
	private List<ByteBuffer> lease(final int size, final int num) throws IOException
	{
		final ArrayList<ByteBuffer> leasedBuffers = new ArrayList<ByteBuffer>(num);
		
		final LinkedList<ByteBuffer> bufferList = buffers.get(size);
		if (bufferList == null)
			throw new IllegalArgumentException("No buffers of size " + size + " were allocated");
		
		if (blocking && num > sizeCountMap.get(size))
			throw new IllegalArgumentException("Not enough buffers allocated of size " + size + 
					" to satisfy this request");
		
		synchronized (bufferList)
		{
			if (blocking)
			{
				while (bufferList.size() < num)
				{
					try
					{
						bufferList.wait();
					} catch (final InterruptedException e)
					{
					}
				}
			}
			final int available = Math.min(num, bufferList.size());
			for (int i = 0; i < available; ++i)
			{
				final ByteBuffer buf = bufferList.remove(0); 
				buf.clear();
				leasedBuffers.add(buf);
			}
		}

		return leasedBuffers;
	}
	
	// TODO: allow multiple release of the same size atomically
	//       otherwise threads requesting a large number of buffers will get starved
	public void put(final ByteBuffer... buffers) throws IOException 
	{
		for (final ByteBuffer buffer : buffers)
			put(buffer);
	}

	public void put(final ByteBuffer buffer) throws IOException
	{
		if (closed)
			throw new IOException("BufferManager is closed");

		if (buffer.capacity() == 0)
			return;

		if (sizeCountMap.isEmpty())
		{
			castle.destroyBuffer(buffer);
			return;
		}

		final LinkedList<ByteBuffer> bufferList = buffers.get(buffer.capacity());
		if (bufferList == null)
		{
			castle.destroyBuffer(buffer);
			return;
		}

		synchronized (bufferList)
		{
			if (bufferList.size() < sizeCountMap.get(buffer.capacity()))
			{
				bufferList.add(buffer);
				if (blocking)
					bufferList.notify();
				return;
			}
		}
		castle.destroyBuffer(buffer);
	}
}
