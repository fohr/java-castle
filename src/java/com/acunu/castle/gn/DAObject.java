package com.acunu.castle.gn;

import java.util.Collection;
import java.util.Iterator;

/**
 * What all objects in a DA have in common.
 * 
 * @author andrewbyde
 */
public abstract class DAObject {
    public static String sysFsRoot = "/sys/fs/castle-fs/vertrees/";
    private final String sysFsString;

	public static String t = "   ";
	public final int daId;

	public DAObject(int daId) {
		this.daId = daId;
		sysFsString = sysFsRoot + Integer.toString(daId, 16);
	}

    public String sysFsString() { return sysFsString; }

	public String toString() {
		return t + "daId     : " + daId + "\n";
	}

	public static int fromHex(String s) {
		return Integer.parseInt(s, 16);
	}

	public static String hex(Integer i) {
	    if (i == null) return "null";
		return Integer.toHexString(i);
	}

	/**
	 * to string for a collection of ints.
	 */
	public static String hex(Collection<Integer> ids) {
		if (ids == null)
			return "null";
		StringBuffer sb = new StringBuffer();
		sb.append("[");
		for (Iterator<Integer> it = ids.iterator(); it.hasNext();) {
			sb.append(hex(it.next()));
			if (it.hasNext())
				sb.append(", ");
		}
		sb.append("]");
		return sb.toString();
	}

	public static String hex(int[] ids) {
		if (ids == null)
			return "null";
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		for (int i = 0; i < ids.length; i++) {
			sb.append("" + hex(ids[i]));
			if (i < ids.length - 1)
				sb.append(", ");
		}
		sb.append("]");
		return sb.toString();
	}

}
