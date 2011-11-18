package com.acunu.castle.control;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static com.acunu.castle.control.HexWriter.*;

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
	public final String ids;

	// list of arrays being output as a result of the merge.
	private List<Long> outputArrayIds;

	// new extent to gather results of drained extents. Null if there are no
	// extents being drained.
	private Long outputValueExtentId = null;

	private long workDone;
	private long workTotal;

	private final String sysFsString;
	public final File sysFsFile;

	public MergeInfo(int daId, int mergeId) {
		super(daId);
		this.id = mergeId;
		this.ids = "M[" + hex(id) + "]";

		sysFsString = super.sysFsString() + hex(mergeId) + "/";
		sysFsFile = new File(sysFsString);
	}

	/**
	 * Constructor in which we copy the basic info from a merge config, and add
	 * some more data now that it has been determined.
	 */
	public MergeInfo(MergeConfig config, int mergeId,
			List<Long> outputArrayIds, Long outputValueExtentId) {
		super(config);
		sysFsString = super.sysFsString() + hex(mergeId) + "/";
		sysFsFile = new File(sysFsString);
		assert (outputArrayIds != null);
		this.id = mergeId;
		this.ids = "M[" + hex(id) + "]";
		this.outputArrayIds = outputArrayIds;
		this.outputValueExtentId = outputValueExtentId;
	}

	public String sysFsString() {
		return sysFsString;
	}

	/** Proportion of the merge that is done. Equates to workDone / workTotal */
	public double progress() {
		return workDone / (double) (workTotal);
	}

	/**
	 * Copy the given merge info.
	 */
	public MergeInfo(MergeInfo mi) {
		super((MergeConfig) mi);
		ids = "M[" + hex(mi.id) + "]";
		sysFsString = super.sysFsString() + hex(mi.id) + "/";
		sysFsFile = new File(sysFsString);
		this.id = mi.id;
		this.outputArrayIds = new ArrayList<Long>(mi.outputArrayIds);
		this.outputValueExtentId = mi.outputValueExtentId;
		workDone = mi.workDone;
		workTotal = mi.workTotal;
	}

	/** Single line description */
	public String toStringLine() {
		return ids + ", " + super.toStringLine() + ", output="
				+ hexL(outputArrayIds) + ", outputVE="
				+ hex(outputValueExtentId) + ", done/total=" + workDone + "/"
				+ workTotal;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(super.toString());
		sb.append(t + "merge-id : " + hex(id) + "\n");
		sb.append(t + "output A : " + hexL(outputArrayIds) + "\n");
		sb.append(t + "output V : " + hex(outputValueExtentId) + "\n");
		sb.append(t + "workDone : " + workDone + "\n");
		sb.append(t + "workLeft : " + workTotal + "\n");
		return sb.toString();
	}

	public void setOutputArrayIds(List<Long> outputArrayIds)
	{
		this.outputArrayIds = outputArrayIds;
	}

	public List<Long> getOutputArrayIds()
	{
		return outputArrayIds;
	}

	public void setOutputValueExtentId(Long outputValueExtentId)
	{
		this.outputValueExtentId = outputValueExtentId;
	}

	public Long getOutputValueExtentId()
	{
		return outputValueExtentId;
	}

	public void setWorkDone(long workDone)
	{
		this.workDone = workDone;
	}

	public long getWorkDone()
	{
		return workDone;
	}

	public void setWorkTotal(long workTotal)
	{
		this.workTotal = workTotal;
	}

	public long getWorkTotal()
	{
		return workTotal;
	}

}
