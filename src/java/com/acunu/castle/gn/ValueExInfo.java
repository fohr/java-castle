package com.acunu.castle.gn;

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

	public ValueExInfo(int daId, int id) {
		super(daId);
		this.id = id;
	}

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
