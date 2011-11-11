package com.acunu.castle.control;

import java.util.HashMap;

/**
 * Castle data holds more than castle info, because DAData holds more than
 * DAInfo.
 */
class CastleData {
	final private HashMap<Integer, DAData> dataMap = new HashMap<Integer, DAData>();

	public void clear() {
		dataMap.clear();
	}

	public void put(int daId, DAData data) {
		dataMap.put(daId, data);
	}

	public DAInfo getInfo(int daId) {
		return dataMap.get(daId);
	}

	public DAData getData(int daId) {
		return dataMap.get(daId);
	}
}