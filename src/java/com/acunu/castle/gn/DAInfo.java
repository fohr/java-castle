package com.acunu.castle.gn;

import java.util.List;
import java.util.Set;

/**
 * Summary of the shape of a single doubling array.
 *
 * @author andrewbyde
 */
public class DAInfo extends DAObject {
	public List<Integer> arrayIds;
	public Set<Integer> valueExIds;
	public List<Integer> mergeIds;

	public DAInfo(int daId, List<Integer> arrayIds, Set<Integer> valueExIds,
			List<Integer> mergeIds) {
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
