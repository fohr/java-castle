package com.acunu.castle.control;

import java.io.File;

import static com.acunu.castle.control.HexWriter.*;
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
	public static String sysFsRoot = "/sys/fs/castle-fs/vertrees/";

	public static final String t = "   ";

	/**
	 * Every DA object has a directory location in sysfs. This is that
	 * directory.
	 */
	private final String sysFsString;

	/** The unique identifier of the DA containing (or equal to) this object. */
	public final int daId;
	public final File daDir;

	/**
	 * Construct a new DA object associated to the DA with id daId.
	 * 
	 * @param daId
	 *            the DA to which this object is associated (or which it is).
	 */
	public DAObject(int daId) {
		this.daId = daId;
		sysFsString = sysFsRoot + hex(daId) + "/";
		daDir = new File(sysFsString);
	}

	/**
	 * Every DA object has a directory location in sysfs. This is that
	 * directory.
	 */
	public String sysFsString() {
		return sysFsString;
	}

	/**
	 * Simple debugging string that reports the da id. Subclasses should append
	 * their own parameters to this, one per line, with the field name padded to
	 * 9 characters followed by colon, space, and value. E.g. "daId     : 1f\n"
	 */
	public String toString() {
		return t + "daId     : " + hex(daId) + "\n";
	}
}
