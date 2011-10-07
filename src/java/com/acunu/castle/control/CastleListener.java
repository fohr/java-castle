package com.acunu.castle.control;

/**
 * A client that controls a nugget server.
 *
 * @author andrewbyde
 */
public interface CastleListener extends DAListener {

	/**
	 * A new DA has been created.
	 */
	public void newDA(DAInfo daInfo);

	/**
	 * All gone...
	 * @param daId the da that has just been destroyed.
	 */
	public void daDestroyed(int daId);
}
