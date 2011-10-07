package com.acunu.castle.control;

import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Summary of the shape of a Nugget-controllable dictionary.
 * 
 * @author andrewbyde
 */
public abstract class CastleInfo {
	public SortedSet<Integer> daIds = new TreeSet<Integer>();

	public abstract DAInfo getInfo(int daId);

	public void clear() {
		daIds.clear();
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (Integer daId : daIds) {
			DAInfo info = getInfo(daId);
			sb.append(daId + " -> \n" + info + "\n");
		}
		return sb.toString();
	}
}