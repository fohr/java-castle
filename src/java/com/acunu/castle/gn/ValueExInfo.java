package com.acunu.castle.gn;

import java.io.File;

/**
 * A 'medium object' value extent.
 * 
 * @author andrewbyde
 */
public class ValueExInfo extends DAObject {
	final public int id;
	
	public int numRqs;
	public long sizeInBytes;
	public long numEntries;

	// TODO hack -- VE should be listed per DA!
	public final static String sysFsRootString = "/sys/fs/castle-fs/data_extents/";
	
	private final String sysFsString;
	final File sysFsFile;

	/**
	 * TODO Note that daId is ignored.
	 */
	public ValueExInfo(int daId, int id) {
		super(daId);
		this.id = id;
		// TODO hack -- should be same as array and merge info
		this.sysFsString = sysFsRootString + hex(id);
		sysFsFile = new File(sysFsString);
	}

	String sysFsString() { return sysFsString; }
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(super.toString());
		sb.append(t + "id       : " + hex(id) + "\n");
		sb.append(t + "rqs      : " + numRqs + "\n");
		sb.append(t + "size     : " + sizeInBytes + "\n");
		sb.append(t + "entries  : " + numEntries + "\n");
		return sb.toString();
	}
}
