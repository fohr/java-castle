package com.acunu.castle.gn;

public class ValueExInfo {

	final public int id;
	public int numRqs;
	public long sizeInBytes;
	public long numEntries;

	public ValueExInfo(int id) {
		this.id = id;
	}

	public String toString() {
        String t = "    ";
		StringBuffer sb = new StringBuffer();
		sb.append(t + "id      : " + id + "\n");
		sb.append(t + "rqs     : " + numRqs + "\n");
		sb.append(t + "size    : " + sizeInBytes + "\n");
		sb.append(t + "entries : " + numEntries + "\n");
		return sb.toString();
	}
}
