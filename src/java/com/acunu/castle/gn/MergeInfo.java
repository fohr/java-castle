package com.acunu.castle.gn;

import java.util.List;

/**
 * Class used to describe an on-going merge.  To specify a new merge, a MergeConfig object is used.
 *
 * @author andrewbyde
 * @see MergeConfig
 */
public class MergeInfo extends MergeConfig {
	
	// unique id of this merge.
	public final int id;

	// list of arrays being output as a result of the merge.
	public List<Integer> outputArrayIds;

	// new extent to gather results of drained extents. Null if there are no extents being drained.
	public Integer outputValueExtentId = null;

	public long workDone;
	public long workLeft;

	/**
	 * Constructor in which we copy the basic info from a merge config, and add some more data now
	 * that it has been determined.
	 */
	public MergeInfo(MergeConfig config, int mergeId,
                     List<Integer> outputArrayIds,
                     Integer outputValueExtentId) {
		super(config);
		assert (outputArrayIds != null);
        this.id = mergeId;
		this.outputArrayIds = outputArrayIds;
		this.outputValueExtentId = outputValueExtentId;
	}
	
	/**
	 * Copy the given merge info.
	 */
	public MergeInfo(MergeInfo mi) {
		this(mi, mi.id, mi.outputArrayIds, mi.outputValueExtentId);
		workDone = mi.workDone;
		workLeft = mi.workLeft;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(super.toString());
		sb.append(t + "merge-id : " + hex(id) + "\n");
		sb.append(t + "output   : " + hex(outputArrayIds) + "\n");
		sb.append(t + "workDone : " + workDone + "\n");
		sb.append(t + "workLeft : " + workLeft + "\n");
		return sb.toString();
	}

}
