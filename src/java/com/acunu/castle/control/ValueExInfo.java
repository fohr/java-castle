package com.acunu.castle.control;

import java.io.File;

import com.acunu.castle.control.ArrayInfo.MergeState;
import com.acunu.util.Utils;

import static com.acunu.castle.control.HexWriter.*;

/**
 * A 'medium object' value extent.
 * 
 * @author andrewbyde
 */
public class ValueExInfo extends DAObject {
	// TODO hack -- VE should be listed per DA!
	public final static String sysFsRootString = "/sys/fs/castle-fs/data_extents/";

	public final long id;
	public final String ids;

	private final String sysFsString;
	
	private int numRqs;
	private long sizeInBytes;
	private long currentSizeInBytes;
	private long numEntries;
	private MergeState mergeState = MergeState.NOT_MERGING;
	
	public final File sysFsFile;

	/**
	 * TODO Note that daId is ignored.
	 */
	public ValueExInfo(int daId, long id) {
		super(daId);
		this.id = id;
		ids = "VE[" + hex(id) + "]";
		// TODO hack -- should be same as array and merge info
		this.sysFsString = sysFsRootString + hex(id);
		sysFsFile = new File(sysFsString);
	}

	public ValueExInfo(ValueExInfo other) {
		this(other.daId, other.id);
		numRqs = other.numRqs;
		sizeInBytes = other.sizeInBytes;
		currentSizeInBytes = other.currentSizeInBytes;
		numEntries = other.numEntries;
		mergeState = other.mergeState;
	}

	/**
	 * The id of the value extent, with the size and either ' ', '-' or '+' appended,
	 * depending on whether the value extent is not merging, an input or an
	 * output, respectively.
	 */
	public String note() {
		String s = Utils.pad(4, hex(id)) + " " + Utils.toStringSize(sizeInBytes);
		 
		if (mergeState == MergeState.OUTPUT) {
			s += "+";
		} else if (mergeState == MergeState.INPUT) {
			s += "-";
		} else
			s += " ";
		s +="|";
		
		return s;
	}

	public String sysFsString() {
		return sysFsString;
	}

	public String toStringLine() {
		return ids + ", size=" + sizeInBytes + ", items=" + numEntries;
	}

	/**
	 * The current size of this extent in bytes.  As data is inserted into the VE
	 * this number grows, and as data is merged out to another extent, this number
	 * shrinks.  It is never larger than 'capacityInBytes'
	 */
	public long sizeInBytes() {
		return sizeInBytes;
	}
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(super.toString());
		sb.append(t + "id       : " + hex(id) + "\n");
		sb.append(t + "rqs      : " + numRqs + "\n");
		sb.append(t + "size     : " + sizeInBytes + "\n");
		sb.append(t + "entries  : " + numEntries + "\n");
		return sb.toString();
	}
	
	public void setNumRqs(int numRqs)
	{
		this.numRqs = numRqs;
	}

	public int getNumRqs()
	{
		return numRqs;
	}

	public void setSizeInBytes(long sizeInBytes)
	{
		this.sizeInBytes = sizeInBytes;
	}

	public long getSizeInBytes()
	{
		return sizeInBytes;
	}

	public void setCurrentSizeInBytes(long currentSizeInBytes)
	{
		this.currentSizeInBytes = currentSizeInBytes;
	}

	public long getCurrentSizeInBytes()
	{
		return currentSizeInBytes;
	}

	public void setNumEntries(long numEntries)
	{
		this.numEntries = numEntries;
	}

	public long getNumEntries()
	{
		return numEntries;
	}

	public void setMergeState(MergeState mergeState)
	{
		this.mergeState = mergeState;
	}

	public MergeState getMergeState()
	{
		return mergeState;
	}
}
