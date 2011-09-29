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

	private final String sysFsString;
	final File sysFsFile;

	public ValueExInfo(int daId, int id) {
		super(daId);
		this.id = id;
		this.sysFsString = super.sysFsString() + "/arrays/"
		+ Integer.toString(id, 16);
		sysFsFile = new File(sysFsString);
	}

	String sysFsString() { return sysFsString; }
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(super.toString());
		sb.append(t + "id       : " + id + "\n");
		sb.append(t + "rqs      : " + numRqs + "\n");
		sb.append(t + "size     : " + sizeInBytes + "\n");
		sb.append(t + "entries  : " + numEntries + "\n");
		return sb.toString();
	}
}
