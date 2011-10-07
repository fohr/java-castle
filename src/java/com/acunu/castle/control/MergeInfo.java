package com.acunu.castle.control;

import java.io.File;
import java.util.List;

/**
 * Class used to describe an on-going merge. To specify a new merge, a
 * MergeConfig object is used.
 * 
 * @author andrewbyde
 * @see MergeConfig
 */
public class MergeInfo extends MergeConfig {

	// unique id of this merge.
	public final int id;

	// list of arrays being output as a result of the merge.
	public List<Integer> outputArrayIds;

	// new extent to gather results of drained extents. Null if there are no
	// extents being drained.
	public Integer outputValueExtentId = null;

	public long workDone;
//	public long workLeft;
	public long workTotal;

	private final String sysFsString;
	final File sysFsFile;

	public MergeInfo(int daId, int mergeId) {
		super(daId);
		this.id = mergeId;
		sysFsString = super.sysFsString() + hex(mergeId) + "/";
		sysFsFile = new File(sysFsString);
	}
	
	/**
	 * Constructor in which we copy the basic info from a merge config, and add
	 * some more data now that it has been determined.
	 */
	public MergeInfo(MergeConfig config, int mergeId,
			List<Integer> outputArrayIds, Integer outputValueExtentId) {
		super(config);
		sysFsString = super.sysFsString() + hex(mergeId) + "/";
		sysFsFile = new File(sysFsString);
		assert (outputArrayIds != null);
		this.id = mergeId;
		this.outputArrayIds = outputArrayIds;
		this.outputValueExtentId = outputValueExtentId;
	}

	public String sysFsString() {
		return sysFsString;
	}

	/** Proportion of the merge that is done.  Equates to workDone / workTotal */
	public double progress() {
		return workDone / (double)(workTotal);
	}
	
	/**
	 * Copy the given merge info.
	 */
	public MergeInfo(MergeInfo mi) {
		super((MergeConfig) mi);
		sysFsString = super.sysFsString() + hex(mi.id) + "/";
		sysFsFile = new File(sysFsString);
		if (mi == null)
			throw new RuntimeException("Cannot copy null MergeInfo");
		this.id = mi.id;
		this.outputArrayIds = mi.outputArrayIds;
		this.outputValueExtentId = mi.outputValueExtentId;
		workDone = mi.workDone;
		workTotal = mi.workTotal;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(super.toString());
		sb.append(t + "merge-id : " + hex(id) + "\n");
		sb.append(t + "output A : " + hex(outputArrayIds) + "\n");
		sb.append(t + "output V : " + hex(outputValueExtentId) + "\n");
		sb.append(t + "workDone : " + workDone + "\n");
		sb.append(t + "workLeft : " + workTotal + "\n");
		return sb.toString();
	}

}
