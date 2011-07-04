package com.acunu.castle;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.acunu.castle.IterStartRequest.IterFlags;

/**
 * Represents a connection to Castle. The methods here are probably thread-safe.
 */
public final class Castle
{
	static
	{
		System.load("/usr/lib64/java-castle/libCastleImpl.so");
	}

	/**
	 * Accessed only from JNI via reflection. Do not remove.
	 */
	@SuppressWarnings("unused")
	private long connectionJNIPointer = 0;
	@SuppressWarnings("unused")
	private long callbackQueueJNIPointer = 0;

	private static final String SYS_FS_DIR = "/sys/fs/castle-fs/";
	private static final String COLLECTIONS_DIR = SYS_FS_DIR + "collections/";

	private native void castle_connect() throws CastleException;

	private native void castle_disconnect() throws CastleException;

	private native void castle_free() throws CastleException;

	private final BufferManager bufferManager;

	private final Thread[] callbackThreads;

	/**
	 * The number of chunks to queue on the ring in put_big before calling
	 * castle_request_blocking. Set to 1 to not do any queueing.
	 */
	private static final int queueChunks = 1;

	public Castle() throws IOException
	{
		this(new HashMap<Integer, Integer>());
	}

	public Castle(Map<Integer, Integer> bufferSizes) throws IOException
	{
		this(bufferSizes, false);
	}

	public Castle(Map<Integer, Integer> bufferSizes, boolean blocking) throws IOException
	{
		castle_connect();

		bufferManager = new BufferManager(this, bufferSizes, blocking);
		callbackThreads = new Thread[10];
		spawnCallbackThreads();
	}

	@Override
	public void finalize() throws IOException
	{
		castle_free();
	}

	private boolean disconnected = false;

	public synchronized void disconnect() throws IOException
	{
		if (!disconnected)
		{
			stopCallbackThreads();
			bufferManager.close();

			castle_disconnect();
			disconnected = true;
		}
	}
	
	/*
	 * Callback functions 
	 */
	private native void callback_queue_shutdown();
	private native void callback_thread_run();
	
	private void spawnCallbackThreads()
	{
		for(int i = 0; i < callbackThreads.length; ++i)
		{
			callbackThreads[i] = new Thread("Castle callback for 0x" + Long.toHexString(connectionJNIPointer)
					+ " thread " + i)
			{
				@Override
				public void run()
				{
					callback_thread_run();
				}
			};
			callbackThreads[i].start();
		}
	}
	
	private void stopCallbackThreads()
	{	
		callback_queue_shutdown();
		
		for(final Thread thread : callbackThreads)
		{
			try
			{
				thread.join();
			} catch (InterruptedException e)
			{
			}
		}
	}

	/*
	 * Some helper functions
	 */

	/**
	 * Read the entire contents of a file that may be padded at the end with
	 * zero bytes.
	 */
	private static String readPaddedFile(File file) throws IOException
	{
		BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		String line = in.readLine();
		in.close();

		int i = line.indexOf(0);
		if (i >= 0)
		{
			line = line.substring(0, i);
		}

		return line;
	}

	private static File getCollectionMapDir() throws IOException
	{
		File collectionMapDir = new File(COLLECTIONS_DIR);
		if (!collectionMapDir.exists())
		{
			throw new IOException("Can't find collection map dir in " + COLLECTIONS_DIR);
		}
		return collectionMapDir;
	}

	/**
	 * Finds the id of the collection with the given name. -1 means no such
	 * collection was found.
	 */
	public static int getCollectionWithName(String name) throws IOException
	{
		File collectionMapDir = getCollectionMapDir();

		for (File id : collectionMapDir.listFiles())
		{
			if (id.isDirectory())
			{
				File nameFile = new File(id, "name");
				String nameInFile = readPaddedFile(nameFile);
				if (name.equals(nameInFile))
				{
					// parse as hexadecimal
					return Integer.parseInt(id.getName(), 16);
				}
			}
		}

		// we haven't found it
		return -1;
	}

	/**
	 * Get the collection with the given name. If it doesn't exist then create
	 * it. Guaranteed to return a valid collection id. Exception is thrown in
	 * the event of an error.
	 */
	public int getOrCreateCollectionWithName(final String name) throws IOException
	{
		int collection = getCollectionWithName(name);
		if (collection != -1)
			return collection;

		final int version = create();
		try
		{
			collection = collection_attach(version, name);
		} catch (final CastleException e)
		{
			if (e.getErrno() == -17 /* file exists */)
			{
				destroyTree(version);
				collection = getCollectionWithName(name);
			}
			if (collection == -1)
				throw e;
		}

		return collection;
	}
	
	public static int getVersionForCollection(final int collection) throws IOException
	{
		final File collectionDir = new File(getCollectionMapDir(), Integer.toString(collection, 16));
		if (!collectionDir.exists())
			throw new FileNotFoundException(collectionDir.getAbsolutePath());
		
		final File versionFile = new File(collectionDir, "version");
		String version = readPaddedFile(versionFile);
		if (version.startsWith("0x"))
			version = version.substring(2);
		return Integer.parseInt(version, 16);
	}

	/*
	 * Control messages
	 */
	private native int castle_claim(int dev) throws CastleException;

	private native void castle_release(int id) throws CastleException;

	private native int castle_attach(int version) throws CastleException;

	private native void castle_detach(int dev) throws CastleException;

	private native int castle_snapshot(int dev) throws CastleException;

	private native int castle_collection_attach_str(int version, String name) throws CastleException;

	private native void castle_collection_detach(int collection) throws CastleException;

	private native int castle_collection_snapshot(int collection) throws CastleException;

	private native int castle_create(long size) throws CastleException;

	private native void castle_destroy_vertree(int vertree) throws CastleException;

	private native void castle_delete_version(int version) throws CastleException;

	private native int castle_clone(int version) throws CastleException;

	private native void castle_init() throws CastleException;

	public int claim(int device) throws IOException
	{
		return castle_claim(device);
	}

	public void release(int disk_id) throws IOException
	{
		castle_release(disk_id);
	}

	public int attach(int version) throws IOException
	{
		return castle_attach(version);
	}

	public void detach(int device_id) throws IOException
	{
		castle_detach(device_id);
	}

	public int create() throws IOException
	{
		return create(0);
	}

	/**
	 * @param size
	 *            Must be 0
	 * @return The id of the new version
	 * @throws IOException
	 */
	public int create(long size) throws IOException
	{
		return castle_create(size);
	}

	/**
	 * Destroys an entire doubling array
	 * 
	 * @param version
	 *            Must be the root version
	 * @throws IOException
	 */
	public void destroyTree(int vertree) throws IOException
	{
		castle_destroy_vertree(vertree);
	}

	/**
	 * Deletes a version from the version tree
	 * 
	 * @param version
	 * @throws IOException
	 */
	public void destroyVersion(int version) throws IOException
	{
		castle_delete_version(version);
	}

	public int clone(int version) throws IOException
	{
		return castle_clone(version);
	}

	public int snapshot(int device_id) throws IOException
	{
		return castle_snapshot(device_id);
	}

	public void init() throws IOException
	{
		castle_init();
	}

	public int collection_attach(int version, String name) throws IOException
	{
		return castle_collection_attach_str(version, name);
	}

	public void collection_detach(int collection_id) throws IOException
	{
		castle_collection_detach(collection_id);
	}

	public int collection_snapshot(int collection_id) throws IOException
	{
		return castle_collection_snapshot(collection_id);
	}

	/*
	 * Iterator helper functions
	 */
	private native Key castle_get_key(ByteBuffer buffer) throws CastleException;

	private native ByteBuffer castle_get_value(ByteBuffer buffer) throws CastleException;

	private native long castle_get_value_length(ByteBuffer buffer) throws CastleException;

	private native ByteBuffer castle_get_next_kv(ByteBuffer buffer) throws CastleException;

	/*
	 * Data path
	 */
	private native ByteBuffer castle_buffer_create(long size) throws CastleException;

	private native void castle_buffer_destroy(ByteBuffer buffer) throws CastleException;

	/* Blocking */
	private native RequestResponse castle_request_blocking(Request request) throws CastleException;

	private native RequestResponse[] castle_request_blocking_multi(Request[] request) throws CastleException;

	/* Non-blocking */
	private native void castle_request_send_multi(Request[] requests, Callback[] callbacks) throws CastleException;

	private void castle_request_send(Request request, Callback callback) throws CastleException
	{
		castle_request_send_multi(new Request[] { request }, new Callback[] { callback });
	}

	public static final int MAX_KEY_SIZE = Key.MAX_KEY_SIZE;

	private static final int INITIAL_GET_SIZE = 4096;
	private static final int KEY_BUFFER_SIZE = MAX_KEY_SIZE;
	private static final int ITER_BUFFER_SIZE = 4096;
	private static final int MAX_INLINE_VALUE_SIZE = 512;

	public static final int MAX_BUFFER_SIZE = 1024 * 1024;
	public static final int MIN_BIG_PUT_SIZE = MAX_INLINE_VALUE_SIZE + 1;
	public static final int MAX_SMALL_PUT_SIZE = MAX_BUFFER_SIZE;
	
	private static final int COUNTER_SIZE = Long.SIZE / 8;

	/* package private */
	BufferManager getBufferManager()
	{
		return bufferManager;
	}
	
	public ByteBuffer createBuffer(int size) throws IOException
	{
		return castle_buffer_create(size);
	}

	public ByteBuffer getBuffer(int size) throws IOException
	{
		return bufferManager.get(size);
	}
	
	public ByteBuffer[] getBuffers(Integer... sizes) throws IOException
	{
		return bufferManager.get(sizes);
	}

	public void putBuffer(ByteBuffer buf) throws IOException
	{
		bufferManager.put(buf);
	}
	
	public void putBuffers(ByteBuffer... bufs) throws IOException
	{
		bufferManager.put(bufs);
	}

	public void destroyBuffer(ByteBuffer buffer) throws IOException
	{
		castle_buffer_destroy(buffer);
	}

	public void put(int collection, Key key, byte[] value) throws IOException
	{
		put(collection, key, value, null);
	}

	public void put(int collection, Key key, byte[] value, Callback callback) throws IOException
	{
		if (value.length > MAX_BUFFER_SIZE)
		{
			put_big(collection, key, value);
			return;
		}

		ByteBuffer[] buffers = null;
		try
		{
			buffers = bufferManager.get(KEY_BUFFER_SIZE, value.length);
			if (callback != null)
				callback.collect(bufferManager, buffers);
			
			buffers[1].put(value);
			buffers[1].flip();
			put(collection, key, buffers[0], buffers[1], callback);
		} catch (IOException e)
		{
			if (callback != null)
				callback.cleanup();
			throw e;
		} catch (Throwable t)
		{
			if (callback != null)
				callback.cleanup();
			throw new RuntimeException(t);
		} finally
		{
			if (buffers != null && callback == null)
				bufferManager.put(buffers);
		}
	}

	public void put(int collection, Key key, ByteBuffer keyBuffer, ByteBuffer valueBuffer) throws IOException
	{
		put(collection, key, keyBuffer, valueBuffer, null);
	}

	/**
	 * Replaces valueBuffer.remaining() bytes under the given key. The number of
	 * bytes must <= MAX_BUFFER_SIZE.
	 */
	public void put(int collection, Key key, ByteBuffer keyBuffer, ByteBuffer valueBuffer, Callback callback) 
			throws IOException
	{
		if (valueBuffer.remaining() > MAX_BUFFER_SIZE)
			throw new IllegalArgumentException("valueBuffer.remaining() " + valueBuffer.remaining()
					+ " > MAX_BUFFER_SIZE " + MAX_BUFFER_SIZE);		
		try
		{
			Request replaceRequest = new ReplaceRequest(key, collection, keyBuffer, valueBuffer);

			if (callback != null)
				castle_request_send(replaceRequest, callback);
			else
				castle_request_blocking(replaceRequest);

		} catch (final IOException e)
		{
			if (callback != null)
				callback.cleanup();
			throw e;
		} catch (final Throwable t)
		{
			if (callback != null)
				callback.cleanup();
			throw new RuntimeException(t);
		}
	}

	/**
	 * A wrapper for big put that can insert values of any length stored in
	 * value
	 */
	private void put_big(int collection, Key key, byte[] value) throws IOException
	{
		ByteBuffer srcBuf = ByteBuffer.wrap(value);
		ByteBuffer[] chunkBuffers = null;
		
		Integer[] buffSizes = new Integer[queueChunks];
		Arrays.fill(buffSizes, MAX_BUFFER_SIZE);

		int numChunks = (int) Math.ceil(((double) value.length) / MAX_BUFFER_SIZE);
		try
		{
			BigPutReply reply = big_put(collection, key, value.length);

			// Init the chunk buffers
			chunkBuffers = bufferManager.get(buffSizes);

			List<PutChunkRequest> reqs = new LinkedList<PutChunkRequest>();
			int remaining = value.length;
			for (int i = 0; i < numChunks; i++)
			{
				int chunkLength;
				boolean lastChunk = remaining <= MAX_BUFFER_SIZE;
				if (remaining > MAX_BUFFER_SIZE)
					chunkLength = MAX_BUFFER_SIZE;
				else
					chunkLength = remaining;
				remaining -= chunkLength;

				ByteBuffer chunkBuffer = chunkBuffers[i % queueChunks];
				chunkBuffer.clear();
				srcBuf.limit(srcBuf.position() + chunkLength);
				assert (srcBuf.remaining() > 0);
				chunkBuffer.put(srcBuf);
				chunkBuffer.flip();

				reqs.add(new PutChunkRequest(reply.token, chunkBuffer));

				if (lastChunk || reqs.size() == queueChunks)
				{
					castle_request_blocking_multi(reqs.toArray(new Request[0]));
					reqs.clear();
				}
			}
		} finally
		{
			if (chunkBuffers != null)
				bufferManager.put(chunkBuffers);
		}
	}

	private void put_multi(int collection, Map<Key, byte[]> values, int totalKeyLength, int totalValueLength, MultiCallback callback)
			throws IOException
	{
		if (totalKeyLength > MAX_BUFFER_SIZE || totalValueLength > MAX_BUFFER_SIZE)
			throw new IOException("Buffers too large");

		if (totalKeyLength == 0)
			throw new IOException("Cannot have key length 0");

		Set<Entry<Key, byte[]>> valueSet = values.entrySet();
		
		ByteBuffer[] buffers = bufferManager.get(totalKeyLength, totalValueLength);
		try
		{
			if (callback != null)
				callback.collect(bufferManager, buffers);
			ByteBuffer keyBuffer = buffers[0];
			ByteBuffer valueBuffer = buffers[1];
			Request[] replaceRequest = new Request[values.size()];
			int i = 0;

			for (Entry<Key, byte[]> entry : valueSet)
			{
				byte[] value = entry.getValue();
				ByteBuffer currentSlice = valueBuffer.slice();
				currentSlice.put(value);
				currentSlice.flip();

				replaceRequest[i] = new ReplaceRequest(entry.getKey(), collection, keyBuffer, currentSlice);

				keyBuffer.position(keyBuffer.position() + entry.getKey().getApproximateLength());
				valueBuffer.position(valueBuffer.position() + value.length);
				i++;
			}

			if (callback == null)
				castle_request_blocking_multi(replaceRequest);
			else
				castle_request_send_multi(replaceRequest, callback.getCallbacks(replaceRequest.length));
		} finally
		{
			if (callback == null)
				bufferManager.put(buffers);
		}
	}

	public void put_multi(int collection, Map<Key, byte[]> values) throws IOException
	{
		put_multi(collection, values, null);
	}
	
	public void put_multi(int collection, Map<Key, byte[]> values, MultiCallback callback) throws IOException
	{
		HashMap<Key, byte[]> valuesToPut = new HashMap<Key, byte[]>();
		Set<Entry<Key, byte[]>> valueSet = values.entrySet();

		int totalKeyLength = 0;
		int totalValueLength = 0;

		for (Entry<Key, byte[]> entry : valueSet)
		{
			if (entry.getValue().length > MAX_BUFFER_SIZE)
			{
				put(collection, entry.getKey(), entry.getValue());
			} else
			{
				int keyLength = entry.getKey().getApproximateLength();
				int valueLength = entry.getValue().length;

				if (totalKeyLength + keyLength <= MAX_BUFFER_SIZE && totalValueLength + valueLength <= MAX_BUFFER_SIZE)
				{
					totalKeyLength += keyLength;
					totalValueLength += valueLength;

					valuesToPut.put(entry.getKey(), entry.getValue());
				} else
				{
					put_multi(collection, valuesToPut, totalKeyLength, totalValueLength, callback);

					valuesToPut.clear();

					totalKeyLength = keyLength;
					totalValueLength = valueLength;

					valuesToPut.put(entry.getKey(), entry.getValue());
				}
			}
		}

		if (!valuesToPut.isEmpty())
			put_multi(collection, valuesToPut, totalKeyLength, totalValueLength, callback);
	}

	public void delete(int collection, Key key) throws IOException
	{
		ByteBuffer keyBuffer = null;
		try
		{
			keyBuffer = bufferManager.get(KEY_BUFFER_SIZE);
			Request removeRequest = new RemoveRequest(key, collection, keyBuffer);

			castle_request_blocking(removeRequest);
		} finally
		{
			if (keyBuffer != null)
				bufferManager.put(keyBuffer);
		}
	}

	/*
	 * length is the maximum length to return. The true value length is
	 * returned in the KeyValue object member valueLength.
	 */
	public KeyValue get(int collection, Key key, int length) throws IOException
	{
		ByteBuffer[] buffers = bufferManager.get(KEY_BUFFER_SIZE, length);
		
		try
		{
			ByteBuffer keyBuffer = buffers[0];
			ByteBuffer valueBuffer = buffers[1];
			// Limit in case we were returned a bigger buffer than necessary
			valueBuffer.limit(length);
			Request getRequest = new GetRequest(key, collection, keyBuffer, valueBuffer);

			RequestResponse response = castle_request_blocking(getRequest);
			if (response.found == false)
				return null;

			int returnedLength = (int) Math.min(response.length, length);
			byte[] value = new byte[returnedLength];
			valueBuffer.get(value, 0, returnedLength);

			return new KeyValue(key, value, response.length);
		} finally
		{
			bufferManager.put(buffers);
		}
	}

	/**
	 * Let r = dest.remaining() and s = the size of the value for the given key,
	 * and m = min(r, s). If a value is found for the key, puts m bytes of the
	 * value into dest, starting at dest.position(). Sets dest.limit() to
	 * dest.position() + m; does NOT modify dest.position(); If no value is
	 * found for the key, makes no changes to dest. Returns s, or -1 if no value
	 * was found for the given key.
	 */
	public long get(int collection, Key key, ByteBuffer keyBuffer, ByteBuffer dest) throws IOException
	{
		Request getRequest = new GetRequest(key, collection, keyBuffer, dest);

		RequestResponse response = castle_request_blocking(getRequest);
		if (response.found == false)
			return -1;

		int m = (int) Math.min(dest.remaining(), response.length);
		dest.limit(dest.position() + m);
		return response.length;
	}

	/**
	 * Convenience function. Gets the value whatever the length, allocating
	 * ByteBuffers as necessary. Will blow up on values that don't fit in RAM.
	 */
	public byte[] get(int collection, Key key) throws IOException
	{
		ByteBuffer[] buffers = null;
		try
		{
			buffers = bufferManager.get(KEY_BUFFER_SIZE, INITIAL_GET_SIZE);
			// Limit in case we were returned a bigger buffer than necessary
			buffers[1].limit(INITIAL_GET_SIZE);
			Request getRequest = new GetRequest(key, collection, buffers[0], buffers[1]);

			RequestResponse response = castle_request_blocking(getRequest);
			if (response.found == false)
				return null;

			if (response.length > INITIAL_GET_SIZE)
			{
				bufferManager.put(buffers);
				buffers = null;
				
				if (response.length > MAX_BUFFER_SIZE)
					return get_big(collection, key);
				
				buffers = bufferManager.get(KEY_BUFFER_SIZE, (int) response.length);
				// Limit in case we were returned a bigger buffer than necessary
				buffers[1].limit((int) response.length);
				getRequest = new GetRequest(key, collection, buffers[0], buffers[1]);

				response = castle_request_blocking(getRequest);
			}

			byte[] value = new byte[(int) response.length];
			buffers[1].get(value, 0, (int) response.length);

			return value;
		} finally
		{
			if (buffers != null)
				bufferManager.put(buffers);
		}
	}

	/* a wrapper for big get that can get values of any length provided they fit into memory */
	private byte[] get_big(int collection, Key key) throws IOException
	{
		ByteBuffer chunkBuffer = null;

		try
		{
			BigGetReply reply = big_get(collection, key);
			int length = (int) reply.length;
			long token = reply.token;
			int offset = 0;

			byte[] value = new byte[length];
			chunkBuffer = bufferManager.get(MAX_BUFFER_SIZE);

			int numChunks = (int) Math.ceil((double) length / (double) MAX_BUFFER_SIZE);

			for (int i = 0; i < numChunks; i++)
			{
				chunkBuffer.clear();
				GetChunkRequest getChunkRequest = new GetChunkRequest(token, chunkBuffer);
				RequestResponse response = castle_request_blocking(getChunkRequest);

				chunkBuffer.get(value, offset, (int) response.length);
				offset += (int) response.length;
			}

			return value;

		} finally
		{
			if (chunkBuffer != null)
				bufferManager.put(chunkBuffer);
		}
	}

	/*
	 * Will only return inline values
	 */
	public Map<Key, byte[]> get_multi(int collection, List<Key> keys) throws IOException
	{
		int totalKeyLength = 0;
		int totalValueLength = 0;

		ArrayList<Key> keysToGet = new ArrayList<Key>();
		HashMap<Key, byte[]> results = new HashMap<Key, byte[]>();

		for (Key key : keys)
		{
			int keyLength = key.getPackedLength();
			int valueLength = MAX_INLINE_VALUE_SIZE;

			if (totalKeyLength + keyLength < MAX_BUFFER_SIZE && totalValueLength + valueLength < MAX_BUFFER_SIZE)
			{
				totalKeyLength += keyLength;
				totalValueLength += valueLength;

				keysToGet.add(key);
			} else
			{
				results.putAll(get_multi(collection, keysToGet, totalKeyLength, totalValueLength));

				keysToGet.clear();

				totalKeyLength = keyLength;
				totalValueLength = valueLength;

				keysToGet.add(key);
			}
		}

		if (!keysToGet.isEmpty())
			results.putAll(get_multi(collection, keysToGet, totalKeyLength, totalValueLength));

		return results;
	}

	private Map<Key, byte[]> get_multi(int collection, List<Key> keys, int totalKeyLength, int totalValueLength)
			throws IOException
	{
		if (totalKeyLength > MAX_BUFFER_SIZE || totalValueLength > MAX_BUFFER_SIZE)
			throw new IOException("Buffers too large");

		if (totalKeyLength == 0)
			throw new IllegalArgumentException("totalKeyLength must be > 0");

		if (totalValueLength == 0)
			throw new IllegalArgumentException("totalValueLength must be > 0");

		HashMap<Key, byte[]> results = new HashMap<Key, byte[]>();

		ByteBuffer keyBuffer = null;
		ByteBuffer valueBuffer = null;
		ByteBuffer[] buffers = null;
		try
		{
			buffers = bufferManager.get(totalKeyLength, totalValueLength);
			keyBuffer = buffers[0];
			valueBuffer = buffers[1];

			Request[] getRequests = new Request[keys.size()];
			int i = 0;

			for (Key key : keys)
			{
				valueBuffer.limit(valueBuffer.position() + MAX_INLINE_VALUE_SIZE);
				getRequests[i] = new GetRequest(key, collection, keyBuffer, valueBuffer);

				keyBuffer.position(keyBuffer.position() + key.getPackedLength());
				valueBuffer.position(valueBuffer.position() + MAX_INLINE_VALUE_SIZE);
				i++;
			}

			RequestResponse[] responses = castle_request_blocking_multi(getRequests);

			int valueOffset = 0;

			for (i = 0; i < keys.size(); i++)
			{
				Key key = keys.get(i);
				RequestResponse response = responses[i];

				if (response.found)
				{
					byte[] value = new byte[(int) Math.min((long) MAX_INLINE_VALUE_SIZE, response.length)];
					valueBuffer.position(valueOffset);
					valueBuffer.get(value);

					results.put(key, value);
				}
				valueOffset += MAX_INLINE_VALUE_SIZE;
			}

			return results;

		} finally
		{
			if (buffers != null)
				bufferManager.put(buffers);
		}
	}

	public List<KeyValue> get_slice(int collection, Slice subspace) throws IOException
	{
		return get_slice(collection, subspace, 0);
	}

	/*
	 * Gets values whatever the size
	 */
	public List<KeyValue> get_slice(int collection, Slice subspace, int limit) throws IOException
	{
		KeyValueIterator iter = getKeyValueIterator(collection, subspace.minKey, subspace.maxKey, ITER_BUFFER_SIZE);
		ArrayList<KeyValue> valueList = new ArrayList<KeyValue>(limit);

		while (iter.hasNext() && (limit == 0 || valueList.size() < limit))
		{
			KeyValue kv = iter.next();
			if (!kv.hasCompleteValue())
				// get out of line value
				kv.setValue(get(collection, kv.getKey()), kv.getValueLength());
			valueList.add(kv);
		}
		iter.close();

		return valueList;
	}

	public KeyValueIterator getKeyValueIterator(int collection, Key keyStart, Key keyFinish) throws IOException
	{
		return new KeyValueIterator(this, collection, keyStart, keyFinish, MAX_BUFFER_SIZE, IterFlags.NONE);
	}

	public KeyValueIterator getKeyValueIterator(int collection, Key keyStart, Key keyFinish, int bufferSize)
			throws IOException
	{
		return new KeyValueIterator(this, collection, keyStart, keyFinish, bufferSize, IterFlags.NONE);
	}

	public KeyValueIterator getKeyValueIterator(int collection, Key keyStart, Key keyFinish, int bufferSize,
			IterFlags flags) throws IOException
	{
		return new KeyValueIterator(this, collection, keyStart, keyFinish, bufferSize, flags);
	}

	public KeyValueIterator getKeyValueIterator(int collection, Key keyStart, Key keyFinish, int bufferSize,
			int numBuffers, IterFlags flags) throws IOException
	{
		return new KeyValueIterator(this, collection, keyStart, keyFinish, bufferSize, numBuffers, flags);
	}

	public IterReply iterstart(int collection, Key keyStart, Key keyFinish) throws IOException
	{
		return iterstart(collection, keyStart, keyFinish, IterFlags.NONE, null);
	}

	public IterReply iterstart(int collection, Key keyStart, Key keyFinish, Callback callback) throws IOException
	{
		return iterstart(collection, keyStart, keyFinish, IterFlags.NONE, callback);
	}

	public IterReply iterstart(int collection, Key keyStart, Key keyFinish, IterFlags flags) throws IOException
	{
		return iterstart(collection, keyStart, keyFinish, flags, null);
	}

	public IterReply iterstart(int collection, Key keyStart, Key keyFinish, IterFlags flags, Callback callback)
			throws IOException
	{
		ByteBuffer[] keyBuffers = null;
		Request iterStartRequest;
		try
		{
			keyBuffers = bufferManager.get(KEY_BUFFER_SIZE, KEY_BUFFER_SIZE);
			
			// fix up infinite keys
			Key start = new Key(new byte[keyStart.key.length][]);
			for (int i = 0; i < keyStart.key.length; i++)
			{
				if (keyStart.key[i].length == 0)
					start.key[i] = Key.MINUS_INF;
				else
					start.key[i] = keyStart.key[i];
			}
			
			Key finish = new Key(new byte[keyFinish.key.length][]);
			for (int i = 0; i < keyFinish.key.length; i++)
			{
				if (keyFinish.key[i].length == 0)
					finish.key[i] = Key.PLUS_INF;
				else
					finish.key[i] = keyFinish.key[i];
			}
			
			iterStartRequest = new IterStartRequest(start, finish, collection, keyBuffers[0], keyBuffers[1],
					flags);

			if (callback != null)
			{
				callback.collect(bufferManager, keyBuffers);

				// callback has been set up so stop the buffers being freed
				keyBuffers = null;

				try
				{
					castle_request_send(iterStartRequest, callback);
					return null;
				} catch (IOException e)
				{
					callback.cleanup();
					throw e;
				} catch (Throwable e)
				{
					callback.cleanup();
					throw new IOException(e);
				}
			} else
			{
				RequestResponse response = castle_request_blocking(iterStartRequest);
				return new IterReply(response.token, null);
			}
		} finally
		{
			if (keyBuffers != null)
				bufferManager.put(keyBuffers);
		}
	}

	public IterReply iternext(long token, int bufferSize) throws IOException
	{
		return iternext(token, bufferSize, null);
	}

	private ArrayList<KeyValue> bufferToKvList(final ByteBuffer buffer) throws IOException
	{
		ArrayList<KeyValue> kvList = new ArrayList<KeyValue>();
		ByteBuffer kvListBuffer = buffer;
		// null first key means empty
		if (castle_get_key(kvListBuffer) != null)
		{
			while (kvListBuffer != null)
			{
				Key key = castle_get_key(kvListBuffer);
				if (key == null)
				{
					throw new IOException("Got null key");
				}
				long valueLength = castle_get_value_length(kvListBuffer);
				boolean haveLength = valueLength >= 0;

				ByteBuffer valueBuffer = castle_get_value(kvListBuffer);

				assert (!haveLength && valueBuffer == null) || (haveLength);

				KeyValue kv;
				// null value is OK, will be out of line or we didn't ask for the values
				if (valueBuffer == null)
				{
					if (haveLength)
						kv = new KeyValue(key, new byte[0], valueLength);
					else
						kv = new KeyValue(key);
				} else
				{
					assert haveLength;
					// copy it since we're going to free the buffer
					byte[] valueArray = new byte[valueBuffer.remaining()];
					valueBuffer.get(valueArray);
					// length might not by valueBuffer.remaining() because we only have
					// first 512 bytes of the value. Call castle_get_value_length to get it
					// from the value length param
					kv = new KeyValue(key, valueArray, castle_get_value_length(kvListBuffer));
				}
				kvList.add(kv);

				kvListBuffer = castle_get_next_kv(kvListBuffer);
			}
		}

		return kvList;
	}

	public IterReply iternext(final long token, final int bufferSize, final IterNextCallback callback)
			throws IOException
	{
		Request iterNextRequest;

		ByteBuffer buffer = bufferManager.get(bufferSize);
		try
		{
			// Limit in case we were returned a bigger buffer than necessary
			buffer.limit(bufferSize);

			iterNextRequest = new IterNextRequest(token, buffer);

			if (callback != null)
			{
				final ByteBuffer iterBuffer = buffer;
				Callback castleCallback = new Callback()
				{
					public void call(RequestResponse response)
					{
						try
						{
							callback.call(new IterReply(token, bufferToKvList(iterBuffer)));
						} catch (IOException e)
						{
							System.out.println("Unable to deserialize iterator buffer");
							e.printStackTrace();
						}
					}

					public void handleError(int error)
					{
						callback.handleError(error);
					}
				};
				castleCallback.collect(bufferManager, buffer);

				// callback has been set up so stop the buffer being freed
				buffer = null;

				try
				{
					castle_request_send(iterNextRequest, castleCallback);
					return null;
				} catch (Exception e)
				{
					castleCallback.cleanup();
					throw new IOException(e);
				}
			} else
			{
				castle_request_blocking(iterNextRequest);
				return new IterReply(token, bufferToKvList(buffer));
			}
		} finally
		{
			if (buffer != null)
				bufferManager.put(buffer);
		}
	}

	public void iterfinish(long token) throws IOException
	{
		Request iterFinishRequest = new IterFinishRequest(token);

		castle_request_blocking(iterFinishRequest);
	}

	public void iterreplacelast(int token, int index, byte[] value) throws IOException
	{
		throw new RuntimeException("not implemented yet");
	}

	public BigPutReply big_put(int collection, Key key, long valueLength) throws IOException
	{
		if (valueLength < MIN_BIG_PUT_SIZE)
			throw new IOException("valueLength " + valueLength + " is smaller than MIN_BIG_PUT_SIZE "
					+ MIN_BIG_PUT_SIZE);

		ByteBuffer keyBuffer = null;
		try
		{
			keyBuffer = bufferManager.get(KEY_BUFFER_SIZE);
			Request bigPutRequest = new BigPutRequest(key, collection, keyBuffer, valueLength);
			RequestResponse response = castle_request_blocking(bigPutRequest);

			return new BigPutReply(response.token);
		} finally
		{
			if (keyBuffer != null)
				bufferManager.put(keyBuffer);
		}
	}

	public void put_chunks(long token, ByteBuffer[] chunkBuffers) throws IOException
	{
		Request[] putChunkRequests = new PutChunkRequest[chunkBuffers.length];

		for (int i = 0; i < chunkBuffers.length; i++)
		{
			putChunkRequests[i] = new PutChunkRequest(token, chunkBuffers[i]);
		}

		castle_request_blocking_multi(putChunkRequests);
	}

	public void put_chunk(long token, ByteBuffer chunkBuffer) throws IOException
	{
		Request request = new PutChunkRequest(token, chunkBuffer);
		castle_request_blocking(request);
	}

	public BigGetReply big_get(int collection, Key key) throws IOException
	{
		ByteBuffer keyBuffer = null;
		try
		{
			keyBuffer = bufferManager.get(KEY_BUFFER_SIZE);
			Request bigGetRequest = new BigGetRequest(key, collection, keyBuffer);
			RequestResponse response = castle_request_blocking(bigGetRequest);

			return new BigGetReply(response.token, response.found, response.length);
		} finally
		{
			if (keyBuffer != null)
				bufferManager.put(keyBuffer);
		}
	}

	/**
	 * For every ByteBuffer b in chunkBuffers, b.position() MUST be
	 * page-aligned. b.position() is NOT modified; b.limit() is set to the
	 * number of valid bytes copied into that buffer, starting at position().
	 */
	public void get_chunks(long token, ByteBuffer[] chunkBuffers) throws IOException
	{
		Request[] getChunkRequests = new GetChunkRequest[chunkBuffers.length];

		for (int i = 0; i < chunkBuffers.length; i++)
		{
			getChunkRequests[i] = new GetChunkRequest(token, chunkBuffers[i]);
		}

		RequestResponse[] responses = castle_request_blocking_multi(getChunkRequests);
		assert (chunkBuffers.length == responses.length);

		// Now set the limit() in each chunkBuffer
		for (int i = 0; i < chunkBuffers.length; i++)
		{
			chunkBuffers[i].limit((int) responses[i].length);
		}
	}

	/**
	 * chunkBuffer.position() MUST be page-aligned. chunkBuffer.position() is
	 * NOT modified; chunkBuffer.limit() is set to the number of valid bytes
	 * copied into that buffer, starting at position().
	 */
	public void get_chunk(long token, ByteBuffer chunkBuffer) throws IOException
	{
		Request request = new GetChunkRequest(token, chunkBuffer);
		RequestResponse response = castle_request_blocking(request);
		// Now set the limit()
		chunkBuffer.limit((int) response.length);
	}
	
	public void counter_set(final int collection, final Key key, final long value) throws IOException
	{
		final ByteBuffer[] buffers = bufferManager.get(KEY_BUFFER_SIZE, COUNTER_SIZE);
		try
		{
			final ByteBuffer keyBuffer = buffers[0];
			final ByteBuffer valueBuffer = buffers[1];
			valueBuffer.order(ByteOrder.LITTLE_ENDIAN).putLong(value);
			valueBuffer.flip();
			
			final CounterSetRequest request = new CounterSetRequest(key, collection, keyBuffer, valueBuffer);
			castle_request_blocking(request);
		} finally {
			bufferManager.put(buffers);
		}
	}
	
	public void counter_add(final int collection, final Key key, final long delta) throws IOException
	{
		final ByteBuffer[] buffers = bufferManager.get(KEY_BUFFER_SIZE, COUNTER_SIZE);
		try
		{
			final ByteBuffer keyBuffer = buffers[0];
			final ByteBuffer valueBuffer = buffers[1];
			valueBuffer.order(ByteOrder.LITTLE_ENDIAN).putLong(delta);
			valueBuffer.flip();
			
			final CounterAddRequest request = new CounterAddRequest(key, collection, keyBuffer, valueBuffer);
			castle_request_blocking(request);
		} finally {
			bufferManager.put(buffers);
		}
	}
	
	public long counter_get(final int collection, final Key key) throws IOException
	{
		final ByteBuffer[] buffers = bufferManager.get(KEY_BUFFER_SIZE, COUNTER_SIZE);
		try
		{
			final ByteBuffer keyBuffer = buffers[0];
			final ByteBuffer valueBuffer = buffers[1];
			final CounterGetRequest request = new CounterGetRequest(key, collection, keyBuffer, valueBuffer);
			final RequestResponse response = castle_request_blocking(request);
			
			if (!response.found)
				return 0l;
			if (response.length != COUNTER_SIZE)
				throw new CastleException(-34, "counter_get: value length out of bounds");
			
			return valueBuffer.order(ByteOrder.LITTLE_ENDIAN).getLong();
		} finally {
			bufferManager.put(buffers);
		}
	}
}
