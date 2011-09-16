package com.acunu.castle.gn;

import java.util.Map;

/**
 * Summary of the shape of a Nugget-controllable dictionary.
 *
 * @author andrewbyde
 */
public class CastleInfo {
	public Map<Integer, DAInfo> daInfoMap;
	
	public CastleInfo(Map<Integer, DAInfo> daInfoMap) {
		this.daInfoMap = daInfoMap;
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for(Map.Entry<Integer, DAInfo> entry : daInfoMap.entrySet()) {
			sb.append(entry.getKey() + " -> " + entry.getValue()+"\n");
		}
		return sb.toString();
	}
}
