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
	public List<Integer> arrayIds;
	public SortedSet<Integer> valueExIds;
	public SortedSet<Integer> mergeIds;

	public DAInfo(int daId, List<Integer> arrayIds, SortedSet<Integer> valueExIds,
			SortedSet<Integer> mergeIds) {
		super(daId);
		assert (arrayIds != null);
		assert (mergeIds != null);
		this.arrayIds = arrayIds;
		this.valueExIds = valueExIds;
		this.mergeIds = mergeIds;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(super.toString());
		sb.append(t + "Arrays   : " + hex(arrayIds) + "\n");
		sb.append(t + "Merges   : " + hex(mergeIds) + "\n");
		sb.append(t + "Extenz   : " + hex(valueExIds) + "\n");
		return sb.toString();
	}

}
