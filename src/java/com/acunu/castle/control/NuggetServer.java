package com.acunu.castle.control;

import java.io.IOException;

/**
 * The interface that a server must implement in order to be controllable by a
 * Golden Nugget.
 * 
 * @author andrewbyde
 */
public interface NuggetServer {

	/** Set target write bandwidth, MB/s. */
    public void setWriteRate(int daId, double rateMB);

	/** Set target read bandwidth, MB/s. */
    public void setReadRate(int daId, double rateMB);

	/** Observed write rate, MB/s. */
	public double getWriteRate(int daId) throws IOException;

	/**
	 * Return a high-level description of the arrays, merges and value extents
	 * that exist.
	 */
	public CastleInfo getCastleInfo() throws IOException;

	/**
	 * Get array information for a specific array, by id.
	 * 
	 * @see ArrayInfo
	 */
	public ArrayInfo getArrayInfo(int daId, int aid) throws IOException;

	/**
	 * Get merge information for a specific merge, by id.
	 * 
	 * @see MergeInfo
	 */
	public MergeInfo getMergeInfo(int daId, int mid) throws IOException;

	/**
	 * Get value extent information for a specific value extent, by id.
	 */
	public ValueExInfo getValueExInfo(int daId, int vxid) throws IOException;

	/**
	 * Initiate a merge of the given arrays. Result is the id of the new merge.
	 * 
	 * @return id of the new merge
	 */
	public MergeInfo startMerge(MergeConfig mergeConfig) throws IOException;

	/**
	 * Do some work on a merge. Operates asynchronously by creating a work unit
	 * and returning its id.
	 * 
	 * @return a pair consisting of the amount of work done, and whether the
	 *         merge is therefore finished.
	 */
	public int doWork(int daId, int mergeId, long mergeUnits)
			throws IOException;

	/**
	 * Change / update the golden nugget.
	 */
	public void setGoldenNugget(CastleController nugget);

	void terminate() throws IOException;
}
