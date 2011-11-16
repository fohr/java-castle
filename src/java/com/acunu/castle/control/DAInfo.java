package com.acunu.castle.control;

import java.util.List;
import java.util.SortedSet;

import static com.acunu.castle.control.HexWriter.*;

/**
 * Summary of the shape of a single doubling array.
 *
 * @author andrewbyde
 */
public abstract class DAInfo extends DAObject {
	public final String ids;

	public DAInfo(int daId) {
		super(daId);
		ids = "DA[" + hex(daId) + "]";
	}

	public abstract List<Long> getArrayIds();

	public abstract SortedSet<Long> getValueExIds();

	public abstract SortedSet<Integer> getMergeIds();

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(super.toString());
		sb.append(t + "Arrays   : " + hexL(getArrayIds()) + "\n");
		sb.append(t + "Merges   : " + hex(getMergeIds()) + "\n");
		sb.append(t + "Extenz   : " + hexL(getValueExIds()) + "\n");
		return sb.toString();
	}

	public String toStringOneLine() {
		StringBuffer sb = new StringBuffer();
		sb.append("A: ").append(hexL(getArrayIds()));
		sb.append(", M: ").append(hex(getMergeIds()));
		sb.append(", VE: ").append(hexL(getValueExIds()));
		return sb.toString();
	}
}
