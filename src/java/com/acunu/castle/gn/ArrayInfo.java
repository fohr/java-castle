package com.acunu.castle.gn;

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
	
	public ArrayInfo(int daId, int id) {
		super(daId);
		this.id = id;
		this.sysFsString = super.sysFsString() + "/arrays/" + Integer.toString(id, 16);
	}

    public String sysFsString() { return sysFsString; }

	/**
	 * Copy the given object.
	 */
	public ArrayInfo(ArrayInfo info) {
		this(info.daId, info.id);
		this.mergeState = info.mergeState;
		this.itemCount = info.itemCount;
		this.reservedSizeInBytes = info.reservedSizeInBytes;
		this.usedInBytes = info.usedInBytes;
		this.currentSizeInBytes = info.currentSizeInBytes;
	}
	
	/**
	 * Set the merge state.  There are three valid names here: 'idle', 'input' and 'output'.
	 */
	public void setMergeState(String s) {
		if ("idle".equals(s))
			mergeState = MergeState.NOT_MERGING;
		else if ("input".equals(s))
			mergeState = MergeState.INPUT;
		else if ("output".equals(s))
			mergeState = MergeState.OUTPUT;
		else
			throw new IllegalArgumentException("Unknown merge state '" + s + "'");
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
		sb.append(t + "merging       : " + mergeState + "\n");
		sb.append(t + "reserved (b)  : " + reservedSizeInBytes + "\n");
		sb.append(t + "used     (b)  : " + usedInBytes + "\n");
		sb.append(t + "size     (b)  : " + currentSizeInBytes + "\n");
		sb.append(t + "cap (i)       : " + itemCount + "\n");
		return sb.toString();
	}
}
