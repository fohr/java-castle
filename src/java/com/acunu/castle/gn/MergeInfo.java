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
	public MergeId id;

	// list of arrays being output as a result of the merge.
	public List<ArrayId> outputArrayIds;

	// new extent to gather results of drained extents. Null if there are no extents being drained.
	public Integer outputValueExtentId = null;

	public long workDone;
	public long workLeft;

	/**
	 * Constructor in which we copy the basic info from a merge config, and add some more data now
	 * that it has been determined.
	 */
	public MergeInfo(MergeId id,
                     MergeConfig config,
                     List<ArrayId> outputArrayIds,
                     Integer outputValueExtentId) {
		super(config);
        assert(id.daId == config.daId);
        this.id = id;
		this.outputArrayIds = outputArrayIds;
		this.outputValueExtentId = outputValueExtentId;
	}

	public String workString() {
		return id + ", done=" + workDone + ", left=" + workLeft;
	}

	public String toString() {
        String t = "    ";
		StringBuffer sb = new StringBuffer();
		sb.append(t + "merge-id : " + id + "\n");
		sb.append(t + "workDone : " + workDone + "\n");
		sb.append(t + "workLeft : " + workLeft + "\n");
		sb.append(t + "input    : " + inputArrayIds + "\n");
		sb.append(t + "output   : null");
		return sb.toString();
	}

}
