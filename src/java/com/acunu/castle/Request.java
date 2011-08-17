package com.acunu.castle;

/**
 * The abstract superclass of all requests (i.e. operations) that may be sent to
 * Castle.
 */
abstract class Request
{
	static
	{
		System.load("/usr/lib64/java-castle/libCastleImpl.so");
		init_jni();
	}
	
	private static native void init_jni();
	public static native long alloc(int num);
	public static native void free(long reqs);
	
    protected static final int CASTLE_RING_REPLACE = 1;
    protected static final int CASTLE_RING_BIG_PUT = 2;
    protected static final int CASTLE_RING_PUT_CHUNK = 3;
    protected static final int CASTLE_RING_GET = 4;
    protected static final int CASTLE_RING_BIG_GET = 5;
    protected static final int CASTLE_RING_GET_CHUNK = 6;
    protected static final int CASTLE_RING_ITER_START = 7;
    protected static final int CASTLE_RING_ITER_NEXT = 8;
    protected static final int CASTLE_RING_ITER_FINISH = 9;
    protected static final int CASTLE_RING_ITER_SKIP = 10;
    protected static final int CASTLE_RING_REMOVE = 11;
    protected static final int CASTLE_RING_COUNTER_SET_REPLACE = 12;
    protected static final int CASTLE_RING_COUNTER_ADD_REPLACE = 13;
    
    /**
     * Do not modify or remove. Only accessed via JNI.
     */
    @SuppressWarnings("unused")
	private final int tag;
    
    protected Request(int tag)
    {
        this.tag = tag;
    }

    abstract void copy_to(long buffer, int index) throws CastleException;
}
