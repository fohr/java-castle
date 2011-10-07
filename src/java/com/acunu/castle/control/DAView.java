package com.acunu.castle.control;

/**
 * The projection of a server's control functions onto one doubling array.
 * 
 * @author andrewbyde
 */
public interface DAView {

	/** Observed write rate, MB/s. */
	public Double getWriteRate();

	/**
	 * Return a high-level description of the arrays, merges and value extents
	 * that exist.
	 */
	public DAInfo getDAInfo();

	/**
	 * Get array information for a specific array, by id.
	 * 
	 * @see ArrayInfo
	 */
	public ArrayInfo getArrayInfo(int aid);

	/**
	 * Get merge information for a specific merge, by id.
	 * 
	 * @see MergeInfo
	 */
	public MergeInfo getMergeInfo(int mid);

	/**
	 * Get value extent information for a specific value extent, by id.
	 */
	public ValueExInfo getValueExInfo(int vxid);

}
