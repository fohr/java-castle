package com.acunu.castle.control;

import java.util.List;
import java.util.SortedSet;

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
	public SortedSet<Integer> extentsToDrain;
	private final String sysFsString;
	
	public MergeConfig(int daId) {
		super(daId);
        sysFsString = super.sysFsString() + "merges/";		
	}
	/**
	 * Constructor in which all input arrays are merged into one.
	 */
	public MergeConfig(int daId, List<Integer> input, SortedSet<Integer> extentsToDrain) {
        super(daId);
        sysFsString = super.sysFsString() + "merges/";
        
		this.inputArrayIds = input;
		this.extentsToDrain = extentsToDrain;
        assert(inputArrayIds.size() >= 1);
	}

	public MergeConfig(MergeConfig copyMe) {
		super(copyMe.daId);
		sysFsString = super.sysFsString() + "merges/";
       
		this.inputArrayIds = copyMe.inputArrayIds;
		this.extentsToDrain = copyMe.extentsToDrain;
	}

	/** The sys fs root for all merges for this DA */
	public String sysFsString() { return sysFsString; }
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(super.toString());
		sb.append(t + "input   : " + hex(inputArrayIds) + "\n");
		sb.append(t + "drain   : " + hex(extentsToDrain) + "\n");
		return sb.toString();
	}
}
