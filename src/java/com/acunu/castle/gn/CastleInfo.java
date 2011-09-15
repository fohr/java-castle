package com.acunu.castle.gn;

import java.util.List;
import java.util.Set;

/**
 * Summary of the shape of a Nugget-controllable dictionary.
 *
 * @author andrewbyde
 */
public class CastleInfo {
	public List<Long> arrayIds;
	public Set<Integer> valueExIds;
	public List<Long> mergeIds;

	public CastleInfo(List<Long> arrayIds, Set<Integer> valueExIds,
			List<Long> mergeIds) {
		assert (arrayIds != null);
		assert (mergeIds != null);
		this.arrayIds = arrayIds;
		this.valueExIds = valueExIds;
		this.mergeIds = mergeIds;
	}

	public String toString() {
        String t = "    ";
		StringBuffer sb = new StringBuffer();
		sb.append(t + "Arrays: " + arrayIds + "\n");
		sb.append(t + "Merges: " + mergeIds + "\n");
		sb.append(t + "Extenz: " + valueExIds + "\n");
		return sb.toString();
	}
}
