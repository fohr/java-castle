package com.acunu.util;

import java.lang.management.ManagementFactory;
import java.text.DecimalFormat;

import org.apache.log4j.Logger;

public class Utils {
	private static Logger log = Logger.getLogger(Utils.class);
	
	public static int pid() {
		int pid = 0;
		try {
			String pName = ManagementFactory.getRuntimeMXBean().getName();
			int at = pName.indexOf('@');
			if (at > 0) {
				pName = pName.substring(0, at);
			}
			pid = Integer.parseInt(pName);
		} catch (Exception e) {
			log.error("Error getting PID: " + e.getMessage(), e);
		}
		return pid;
	}
	
	/**
	 * Waiting in which we keep track of which thread waited and for how long.
	 * 
	 * @param waitAmount
	 *            delay in milliseconds.
	 */
	public static void waitABit(int waitAmount) {
		try {
			Thread.sleep(waitAmount);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}


	static final long kb = 1024;
	static final long mb = kb*kb;
	static final long gb = kb*mb;
	static final long tb = kb*gb;
	static final long pb = kb*tb;
	static final long eb = kb*pb;
	
	public static final double mbDouble = (double)mb;
	
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
	public static DecimalFormat twoPlaces = new DecimalFormat("0.00");

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
