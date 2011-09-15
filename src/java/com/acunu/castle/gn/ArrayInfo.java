package com.acunu.castle.gn;

import java.util.Set;

/**
 * Summary of the stats of an array managed by a nugget server.
 *
 * @author andrewbyde
 */
public class ArrayInfo {

    public final int id;
	public boolean isMerging = false;

	public long capacityInBytes;
	public long sizeInBytes;

	public ArrayInfo(int id) {
		this.id = id;
	}

	public String toString() {
        String t = "    ";
		StringBuffer sb = new StringBuffer();
		sb.append(t + "id      : " + id + "\n");
		sb.append(t + "merging : " + isMerging + "\n");
		sb.append(t + "size    : " + sizeInBytes + "\n");
		return sb.toString();
	}
}
