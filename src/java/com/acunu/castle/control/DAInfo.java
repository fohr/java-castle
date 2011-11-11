package com.acunu.castle.control;

import java.util.List;
import java.util.SortedSet;

/**
 * Summary of the shape of a single doubling array.
 *
 * @author andrewbyde
 */
public class DAInfo extends DAObject {
	
	// TODO we want a list with set-like properties -- uniqueness of elements.
	public List<Long> arrayIds;
	public SortedSet<Long> valueExIds;
	public SortedSet<Integer> mergeIds;
	public final String ids;
	
	public DAInfo(int daId, List<Long> arrayIds, SortedSet<Long> valueExIds,
			SortedSet<Integer> mergeIds) {
		super(daId);
		ids = "DA[" + hex(daId) + "]";
		
		assert (arrayIds != null);
		assert (mergeIds != null);
		this.arrayIds = arrayIds;
		this.valueExIds = valueExIds;
		this.mergeIds = mergeIds;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(super.toString());
		sb.append(t + "Arrays   : " + hexL(arrayIds) + "\n");
		sb.append(t + "Merges   : " + hex(mergeIds) + "\n");
		sb.append(t + "Extenz   : " + hexL(valueExIds) + "\n");
		return sb.toString();
	}

	public String toStringOneLine() {
		StringBuffer sb = new StringBuffer();
		sb.append("A: ").append(hexL(arrayIds));
		sb.append(", M: ").append(hex(mergeIds));
		sb.append(", VE: ").append(hexL(valueExIds));
		return sb.toString();		
	}
}
