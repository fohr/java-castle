package com.acunu.castle.control;

import java.util.Collection;
import java.util.Iterator;

/**
 * Class to comprehend integers as hex and vice-versa.
 * 
 * @author andrewbyde
 */
public class HexWriter {
	/**
	 * Convert a hex string with no initial '0x', e.g. '4f2' into an integer.
	 */
	public static int fromHex(String s) {
		return Integer.parseInt(s, 16);
	}

	/**
	 * Convert an integer into a hex string.  No initial '0x'.
	 */
	public static String hex(Integer i) {
		if (i == null)
			return "null";
		return Integer.toHexString(i);
	}

	/**
	 * Applies 'hex(Integer)' to a collection of Integers.
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

	/**
	 * Applies 'hex(Integer)' to an array of ints.
	 */
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
