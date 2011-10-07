package com.acunu.castle.control;

import java.io.IOException;

/**
 * The projection of a server's control functions onto one doubling array.
 * 
 * @author andrewbyde
 */
public interface DAControlServer extends DAView {

	/** Set target write bandwidth, MB/s. */
    public void setWriteRate(double rateMB);

	/** Set target read bandwidth, MB/s. */
    public void setReadRate(double rateMB);

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
	public int doWork(int mergeId, long mergeUnits)
			throws IOException;

}
