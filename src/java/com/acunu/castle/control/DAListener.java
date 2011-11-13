package com.acunu.castle.control;

/**
 * A client that controls a nugget server.
 * 
 * @author andrewbyde
 */
public interface DAListener {

	/**
	 * Called by the nugget server whenever a new array is flushed from memory
	 * into the system.
	 * 
	 * @param arrayInfo
	 *            info regarding the new array
	 */
	void newArray(ArrayInfo arrayInfo);

	/**
	 * Called by the nugget server when a piece of work has been completed.
	 * 
	 * @param daId
	 *            the da on which work has just finished.
	 * @param work
	 *            the work unit that has completed
	 * @param isMergeFinished
	 *            whether this work unit finished the corresponding merge or
	 *            not.
	 */
	public void workDone(int daId, MergeWork work,
			boolean isMergeFinished);

	/**
	 * Dispose of this object and all associated threads. Called when the DA
	 * that this listener is attending to is itself disposed of.
	 */
	public void dispose();

}
