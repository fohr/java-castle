package com.acunu.castle.gn;

import java.util.List;
import java.util.Set;

/**
 * Specification of a merge, sent to a nugget server. Where null, metadata
 * should be interpreted to mean 'all' -- in particular if the set of
 * ValueExtents to drain is null, all extents will be drained; if the set of
 * lead versions is null, then one output array will be created for all output
 * and all versions in the input.
 *
 * @author andrewbyde
 */
public class MergeConfig extends DAObject {
    public List<Integer> inputArrayIds;
	public Set<Integer> extentsToDrain = null;

	/**
	 * Constructor in which all input arrays are merged into one.
	 */
	public MergeConfig(int daId, List<Integer> input) {
        super(daId);
		this.inputArrayIds = input;
        assert(inputArrayIds.size() >= 1);
	}

	public MergeConfig(MergeConfig copyMe) {
		super(copyMe.daId);
		this.inputArrayIds = copyMe.inputArrayIds;
		this.extentsToDrain = copyMe.extentsToDrain;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(super.toString());
		sb.append(t + "input   : " + inputArrayIds + "\n");
		sb.append(t + "drain   : " + extentsToDrain + "\n");
		return sb.toString();
	}
}
