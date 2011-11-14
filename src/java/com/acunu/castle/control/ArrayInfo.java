package com.acunu.castle.control;

import java.io.File;
import java.util.Comparator;
import java.util.SortedSet;

import static com.acunu.castle.control.HexWriter.*;

/**
 * Summary of the stats of an array managed by a nugget server.
 * 
 * @author andrewbyde
 */
public class ArrayInfo extends DAObject {
	public final long id;
	public final int dataTime;

	public final String ids;
	public static Comparator<ArrayInfo> dataTimeComparator = new Comparator<ArrayInfo>() {

		/**
		 * Compares its two arguments for order. Returns a negative integer,
		 * zero, or a positive integer as the first argument is less than, equal
		 * to, or greater than the second. Small data time means older, and we
		 * want them at the end of the list, so a comes before a' <=> compare(a,
		 * a') < 0 <=> a'.dataTime < a.dataTime <=> a'.dataTime - a.dataTime <
		 * 0. For tie-breaking we use id, and the opposite is true -- newer
		 * means merge output, which should come to the right of existing array.
		 */
		@Override
		public int compare(ArrayInfo arg0, ArrayInfo arg1) {
			if (arg0.dataTime == arg1.dataTime) {
				long c = arg0.id - arg1.id;
				if (c < 0)
					return -1;
				else if (c > 0)
					return 1;
				else
					return 0;
			} else
				return (arg1.dataTime - arg0.dataTime);
		}
	};

	public enum MergeState {
		INPUT, OUTPUT, NOT_MERGING
	};

	private MergeState mergeState = MergeState.NOT_MERGING;
	private long itemCount;
	private long reservedSizeInBytes;
	private long usedInBytes;
	private long currentSizeInBytes;

	private final String sysFsString;
	public final File sysFsFile;

	private SortedSet<Long> valueExIds;

	public ArrayInfo(int daId, long id, int dataTime) {
		super(daId);
		this.id = id;
		this.dataTime = dataTime;
		ids = "A[" + hex(id) + "]";

		this.sysFsString = super.sysFsString() + "arrays/" + hex(id);
		sysFsFile = new File(sysFsString);
	}

	public String sysFsString() {
		return sysFsString;
	}

	/**
	 * Return how big this is projected to be. While merging in we use the
	 * reserved size; once the merge is finalised or we are merging out, we use
	 * the max size.
	 */
	public long maxInBytes() {
		if (mergeState == MergeState.OUTPUT)
			return this.reservedSizeInBytes;
		return usedInBytes;
	}

	public double progress() {
		return sizeInBytes() / (double) maxInBytes();
	}

	/**
	 * The current size of the array this info represents.
	 */
	public long sizeInBytes() {
		return currentSizeInBytes;
	}

	/**
	 * Copy the given object.
	 */
	public ArrayInfo(ArrayInfo info) {
		this(info.daId, info.id, info.dataTime);

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

	/** single line description */
	public String toStringLine() {
		String s = ids + ", VEs=" + hexL(valueExIds) + ", state=" + mergeState;
		s += ", res/used/size/items=" + reservedSizeInBytes + "/" + usedInBytes
				+ "/" + currentSizeInBytes + "/" + itemCount;
		return s;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(super.toString());
		sb.append(t + "id            : " + hex(id) + "\n");
		sb.append(t + "value extents : " + hexL(valueExIds) + "\n");
		sb.append(t + "merging       : " + mergeState + "\n");
		sb.append(t + "reserved (b)  : " + reservedSizeInBytes + "\n");
		sb.append(t + "used     (b)  : " + usedInBytes + "\n");
		sb.append(t + "size     (b)  : " + currentSizeInBytes + "\n");
		sb.append(t + "items    (i)  : " + itemCount + "\n");
		return sb.toString();
	}

	public void setMergeState(MergeState mergeState)
	{
		this.mergeState = mergeState;
	}

	public MergeState getMergeState()
	{
		return mergeState;
	}

	public void setItemCount(long itemCount)
	{
		this.itemCount = itemCount;
	}

	public long getItemCount()
	{
		return itemCount;
	}

	public void setReservedSizeInBytes(long reservedSizeInBytes)
	{
		this.reservedSizeInBytes = reservedSizeInBytes;
	}

	public long getReservedSizeInBytes()
	{
		return reservedSizeInBytes;
	}

	public void setUsedInBytes(long usedInBytes)
	{
		this.usedInBytes = usedInBytes;
	}

	public long getUsedInBytes()
	{
		return usedInBytes;
	}

	public void setCurrentSizeInBytes(long currentSizeInBytes)
	{
		this.currentSizeInBytes = currentSizeInBytes;
	}

	public long getCurrentSizeInBytes()
	{
		return currentSizeInBytes;
	}

	public void setValueExIds(SortedSet<Integer> valueExIds)
	{
		this.valueExIds = valueExIds;
	}

	public SortedSet<Integer> getValueExIds()
	{
		return valueExIds;
	}
}
