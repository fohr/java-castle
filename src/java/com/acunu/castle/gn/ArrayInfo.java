package com.acunu.castle.gn;

import java.io.File;
import java.util.Collection;

/**
 * Summary of the stats of an array managed by a nugget server.
 * 
 * @author andrewbyde
 */
public class ArrayInfo extends DAObject {
	public final int id;

	public enum MergeState {
		INPUT, OUTPUT, NOT_MERGING
	};

	public MergeState mergeState = MergeState.NOT_MERGING;
	public long itemCount;
	public long reservedSizeInBytes;
	public long usedInBytes;
	public long currentSizeInBytes;

	private final String sysFsString;
	final File sysFsFile;
	
	public Collection<Integer> valueExIds;
	
	public ArrayInfo(int daId, int id) {
		super(daId);
		this.id = id;
		this.sysFsString = super.sysFsString() + "arrays/" + hex(id);
		sysFsFile = new File(sysFsString);
	}

	String sysFsString() {
		return sysFsString;
	}

	/**
	 * Copy the given object.
	 */
	public ArrayInfo(ArrayInfo info) {
		this(info.daId, info.id);
		
		// state
		this.mergeState = info.mergeState;
		
		// size params
		this.itemCount = info.itemCount;
		this.reservedSizeInBytes = info.reservedSizeInBytes;
		this.usedInBytes = info.usedInBytes;
		this.currentSizeInBytes = info.currentSizeInBytes;
		
		// value extents
		this.valueExIds = info.valueExIds;
	}

	/**
	 * Set the merge state. There are three valid names here: 'idle', 'input'
	 * and 'output'.
	 */
	public void setMergeState(String s) {
		if ("idle".equals(s))
			mergeState = MergeState.NOT_MERGING;
		else if ("input".equals(s))
			mergeState = MergeState.INPUT;
		else if ("output".equals(s))
			mergeState = MergeState.OUTPUT;
		else
			throw new IllegalArgumentException("Unknown merge state '" + s
					+ "'");
	}

	public boolean isSource() {
		return mergeState == MergeState.INPUT;
	}

	public boolean isDestination() {
		return mergeState == MergeState.OUTPUT;
	}

	public boolean isMerging() {
		return mergeState != MergeState.NOT_MERGING;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(super.toString());
		sb.append(t + "id            : " + hex(id) + "\n");
		sb.append(t + "value extents : " + hex(valueExIds) + "\n");
		sb.append(t + "merging       : " + mergeState + "\n");
		sb.append(t + "reserved (b)  : " + reservedSizeInBytes + "\n");
		sb.append(t + "used     (b)  : " + usedInBytes + "\n");
		sb.append(t + "size     (b)  : " + currentSizeInBytes + "\n");
		sb.append(t + "items    (i)  : " + itemCount + "\n");
		return sb.toString();
	}
}
