package com.acunu.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeSet;

/**
 * Extend properties to have some useful type-casting methods.
 * 
 * @author andrewbyde
 */
public class Properties extends java.util.Properties {
	private static final long serialVersionUID = 1L;
	private SortedSet<String> sortedParams = new TreeSet<String>();

	/**
	 * Read lines in from a file. Ignore any without a '=' or which start with
	 * '#'
	 */
	public void read(File f) throws IOException {
		BufferedReader r = new BufferedReader(new FileReader(f));
		String line;
		while ((line = r.readLine()) != null) {
			if (line.startsWith("#"))
				continue;

			int x = line.indexOf('=');
			if (x < 0)
				continue;

			String param = line.substring(0, x);
			StringTokenizer st = new StringTokenizer(param);
			param = st.nextToken();

			String value = line.substring(x + 1);
			st = new StringTokenizer(value);
			value = st.nextToken();
			
			setProperty(param, value);
		}
	}

	public Object setProperty(String p, String value) {
		sortedParams.add(p);
		return super.setProperty(p, value);
	}

	public String getProperty(String p, String value) {
		String x = super.getProperty(p);
		if (x == null) {
			setProperty(p, value);
			return value;
		}
		return x;
	}

	public String getProperty2(String p) {
		if (this.containsKey(p))
			return super.getProperty(p);
		throw new IllegalArgumentException("Parameter '" + p + "' not set.");
	}

	public long getLong(String p) {
		return Long.parseLong(getProperty2(p));
	}

	/** Throws an illegal argument exception if p is not set. */
	public int getInt(String p) {
		return Integer.parseInt(getProperty2(p));
	}

	/** Throws an illegal argument exception if p is not set. */
	public double getDouble(String p) {
		return Double.parseDouble(getProperty2(p));
	}

	/** Throws an illegal argument exception if p is not set. */
	public boolean getBoolean(String p) {
		return Boolean.parseBoolean(getProperty2(p));
	}

	public void setInt(String p, int x) {
		setProperty(p, "" + x);
	}

	public void setDouble(String p, double x) {
		setProperty(p, "" + x);
	}

	public void setBoolean(String p, boolean x) {
		setProperty(p, "" + x);
	}

	// defaults

	public int getInt(String p, int x) {
		return Integer.parseInt(getProperty(p, "" + x));
	}

	public double getDouble(String p, double x) {
		return Double.parseDouble(getProperty(p, "" + x));
	}

	public boolean getBoolean(String p, boolean x) {
		return Boolean.parseBoolean(getProperty(p, "" + x));
	}

	public long getLong(String p, long x) {
		return Long.parseLong(getProperty(p, "" + x));
	}

	/** param = value per line, ordered by param. */
	public String toStringOrdered() {
		// first work out appropriate size
		int size = 0;
		for (String param : sortedParams) {
			size = Math.max(size, param.length());
		}

		StringBuffer sb = new StringBuffer();
		for (String param : sortedParams) {
			sb.append("# " + Utils.pad(param, size) + " " + getProperty(param)
					+ "\n");
		}
		return sb.toString();
	}

	/**
	 * For each string in the array given, parse as 'param=value' and call
	 * p.setProperty(param, value) on each.
	 */
	public void parseParams(String[] args) {
		for (String s : args) {
			// split on "="
			int splitIndex = s.indexOf('=');
			if (splitIndex < 0)
				throw new RuntimeException(
						"Parameter '"
								+ s
								+ "' is ill-formed.  It should be of the form 'param=value'");
			String param = s.substring(0, splitIndex);
			String value = s.substring(splitIndex + 1, s.length());
			setProperty(param, value);
		}
	}

	/**
	 * Report this property set as an array of 'param=value' strings.
	 */
	public String[] getArgs() {
		String[] args = new String[sortedParams.size()];
		int i = 0;
		for (String param : sortedParams) {
			args[i++] = param + "=" + getProperty(param);
		}
		return args;
	}
}
