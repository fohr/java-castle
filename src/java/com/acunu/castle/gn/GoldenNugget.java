package com.acunu.castle.gn;

/**
 * A client that controls a nugget server.
 *
 * @author andrewbyde
 */
public interface GoldenNugget {

	/**
	 * Called byt the nugget server whenever a new array is flushed from memory
	 * into the system.
	 *
	 * @param arrayInfo
	 *            info regarding the new array
	 */
	void newArray(ArrayInfo arrayInfo);

	/**
	 * Called by the nugget server when a piece of work has been completed.
	 *
	 * @param workId
	 *            the work unit that has completed
	 * @param workDoneBytes
	 *            the amount of work done, in bytes
	 * @param isMergeFinished
	 *            whether this work unit finished the corresponding merge or
	 *            not.
	 */
	public void workDone(int workId, long workDoneBytes, boolean isMergeFinished);

	public void setServer(NuggetServer server);
}
