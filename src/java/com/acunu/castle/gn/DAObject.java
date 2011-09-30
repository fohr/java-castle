package com.acunu.castle.gn;

import java.text.DecimalFormat;
import java.util.Collection;
import java.util.Iterator;

/**
 * What all objects in a DA have in common.
 * 
 * @author andrewbyde
 */
public abstract class DAObject {

	/**
	 * The sys-fs root directory for all information about objects in a DA. In
	 * particular the directory containing information for the DA with id 'daId'
	 * is 'sysFsRoot' with daId appended.
	 */
	static String sysFsRoot = "/sys/fs/castle-fs/vertrees/";

	/**
	 * Every DA object has a directory location in sysfs. This is that
	 * directory.
	 */
	private final String sysFsString;

	public static String t = "   ";

	/** The unique identifier of the DA containing (or equal to) this object. */
	public final int daId;

	/**
	 * Construct a new DA object associated to the DA with id daId.
	 * 
	 * @param daId
	 *            the DA to which this object is associated (or which it is).
	 */
	public DAObject(int daId) {
		this.daId = daId;
		sysFsString = sysFsRoot + hex(daId) + "/";
	}

	/**
	 * Every DA object has a directory location in sysfs. This is that
	 * directory.
	 */
	String sysFsString() {
		return sysFsString;
	}

	/**
	 * Simple debugging string that reports the da id. Subclasses should append
	 * their own parameters to this, one per line, with the field name padded to
	 * 9 characters followed by colon, space, and value. E.g. "daId     : 1f\n"
	 */
	public String toString() {
		return t + "daId     : " + daId + "\n";
	}

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

	static final long kb = 1024;
	static final long mb = kb*kb;
	static final long gb = kb*mb;
	static final long tb = kb*gb;
	static final long pb = kb*tb;
	static final long eb = kb*pb;
	
	private static String unitString(long units) {
		if (units == 1)
			return " B ";
		else if (units == kb)
			return " kB";
		else if (units == mb)
			return " MB";
		else if (units == gb)
			return " GB";
		else if (units == tb)
			return " TB";
		else if (units == pb)
			return " PB";
		else if (units == eb)
			return " EB";
		else 
			throw new IllegalArgumentException("units of " + units + " are unknown");
	}
	
	public static DecimalFormat onePlace = new DecimalFormat("0.0");

	private static String toStringSize(long n, long units) {
		if (n < 10)
			return pad(6, n + " B ");
		if (n < 1000*units) {
			if (n < 10*units) {
				double x = n/(double)units;
				return onePlace.format(x) + unitString(units);
			} else 
				return pad(3, ""+(n/units)) + unitString(units);
		} else {
			return toStringSize(n, units*kb);
		}
	}
	
	/**
	 * Convert a number of bytes to a 2-place readable description of how big it
	 * is.
	 */
	public static String toStringSize(long n) {
		return toStringSize(n, 1);
	}
	
	static String[] pads = new String[] { "", " ", "  ", "   ", "    ",
			"     ", "      " };

	private static String pad(int length) {
		assert (length >= 0);
		if (length < pads.length)
			return pads[length];
		return pad(length / 2) + pad(length - length / 2);
	}

	public static String pad(int length, String x) {
		if (x == null)
			return pad(length, "null");
		int padSize = length - x.length();
		if (padSize < 0)
			return x;
		return pad(padSize) + x;
	}

	public static String pad(String x, int length) {
		if (x == null)
			return pad(length, "null");
		int padSize = length - x.length();
		if (padSize < 0)
			return x;
		return x + pad(padSize);
	}


}
