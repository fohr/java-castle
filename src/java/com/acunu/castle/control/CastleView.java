package com.acunu.castle.control;

import java.util.SortedSet;

/**
 * The interface that an instance of castle implements in order to be viewable.
 * 
 * @author andrewbyde
 */
public interface CastleView {
	
	/**
	 * The set of available das to view.
	 */
	public SortedSet<Integer> daList();

	/**
	 * Project the view functions of this server onto a particular DA.
	 */
	public DAView projectView(int daId);
}
