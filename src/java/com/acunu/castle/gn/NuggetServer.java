package com.acunu.castle.gn;

import java.io.IOException;

/**
 * The interface that a server must implement in order to be controllable by a
 * Golden Nugget.
 *
 * @author andrewbyde
 */
public interface NuggetServer {

	/** Set target write bandwidth, MB/s. */
	public int setWriteRate(double rateMB);

	/** Set target read bandwidth, MB/s. */
	public int setReadRate(double rateMB);

	/** Observed write rate, MB/s. */
	public double getWriteRate();

	public CastleInfo getCastleInfo();

	public ArrayInfo getArrayInfo(int aid);

	public MergeInfo getMergeInfo(int mid);

	public ValueExInfo getValueExInfo(int vxid);

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
	public int doWork(int mergeId, long mergeUnits) throws IOException;

	/**
	 * Change / update the golden nugget.
	 */
	public void setGoldenNugget(GoldenNugget nugget);

	/**
	 * Returns thread id.
	 * @return
	 */
	int mergeThreadCreate() throws IOException;

	/**
	 * After attach, this merge must be completed.  Thread automatically
	 * detaches when the merge finishes.  Does not start working until "doWork" is called.
	 *
	 * @param mergeId
	 * @param threadId
	 */
	void mergeThreadAttach(int mergeId, int threadId) throws IOException;

	void mergeThreadDestroy(int threadId) throws IOException;

    void terminate() throws IOException;
}
