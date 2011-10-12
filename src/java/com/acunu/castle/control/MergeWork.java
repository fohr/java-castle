package com.acunu.castle.control;

public class MergeWork extends DAObject {
	public MergeInfo mergeInfo;
	public final int workId;
	public final String ids;
	public long mergeUnits;
	public final long startTime = System.currentTimeMillis();

	MergeWork(MergeInfo info, int workId, long mergeUnits) {
		super(info.daId);
		this.mergeInfo = info;
		this.workId = workId;
		this.ids = "W[" + hex(workId) + "]";
		this.mergeUnits = mergeUnits;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append(t + "workId   : " + hex(workId) + "\n");
		sb.append(t + "mergeId  : " + hex(mergeInfo.id) + "\n");
		sb.append(t + "units    : " + mergeUnits + "\n");
		return sb.toString();
	}
}

