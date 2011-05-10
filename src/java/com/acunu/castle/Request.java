package com.acunu.castle;

/**
 * The abstract superclass of all requests (i.e. operations) that may be sent to
 * Castle.
 */
abstract class Request
{
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
    
    /**
     * Do not modify or remove. Only accessed via JNI.
     */
    @SuppressWarnings("unused")
	private final int tag;
    
    protected Request(int tag)
    {
        this.tag = tag;
    }

    protected abstract void copy_to(long buffer) throws CastleException;
}
