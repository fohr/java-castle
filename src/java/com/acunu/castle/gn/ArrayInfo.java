package com.acunu.castle.gn;


/**
 * Summary of the stats of an array managed by a nugget server.
 *
 * @author andrewbyde
 */
public class ArrayInfo extends DAObject {
    public final int id;
    
	public boolean isMerging = false;

	public long capacityInItems;
	public long reservedSizeInBytes;
    public long usedInBytes;
    public long currentSizeInBytes;

	public ArrayInfo(int daId, int id) {
		super(daId);
		this.id = id;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(super.toString());
		sb.append(t + "id            : " + hex(id) + "\n");
		sb.append(t + "merging       : " + isMerging + "\n");
		sb.append(t + "reserved (b)  : " + reservedSizeInBytes + "\n");
		sb.append(t + "used     (b)  : " + usedInBytes + "\n");
		sb.append(t + "size     (b)  : " + currentSizeInBytes + "\n");
		sb.append(t + "cap (i)       : " + capacityInItems + "\n");
		return sb.toString();
	}
}

