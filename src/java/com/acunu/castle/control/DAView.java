package com.acunu.castle.control;

/**
 * The projection of a server's control functions onto one doubling array.
 * 
 * @author andrewbyde
 */
public interface DAView {

	/**
	 * Return a high-level description of the arrays, merges and value extents
	 * that exist.
	 */
	public DAInfo getDAInfo();

	/** Observed write rate, MB/s. */
	public Double getWriteRate();

	/** Observed write rate, longer time scale, MB/s. */
	public Double getWriteRateLong();

	/** Observed total amount written, MB. */
	double totalWritten();

	/** Observed merge rate, MB/s. */
	public Double getMergeRate();

	/** Observed merge rate, longer time scale, MB/s. */
	public Double getMergeRateLong();

	/** Observed total amount merged, MB. */
	double totalMerged();

	/** Observed merge rate, MB/s. */
	public Double getReadRate();

	/** Observed merge rate, longer time scale, MB/s. */
	public Double getReadRateLong();

	/** Observed total amount merged, MB. */
	double totalRead();

	
	/** Target for a ceiling on write rate, MB/s. */
	public Double getWriteCeiling();

	/** Target for a ceiling on write rate, MB/s. */
	public Double getReadCeiling();

	/**
	 * Get array information for a specific array, by id.
	 * 
	 * @see ArrayInfo
	 */
	public ArrayInfo getArrayInfo(long aid);

	/**
	 * Get merge information for a specific merge, by id.
	 * 
	 * @see MergeInfo
	 */
	public MergeInfo getMergeInfo(int mid);

	/**
	 * Get value extent information for a specific value extent, by id.
	 */
	public ValueExInfo getValueExInfo(long vxid);

}
