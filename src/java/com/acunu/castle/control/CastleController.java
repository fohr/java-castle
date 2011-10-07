package com.acunu.castle.control;

/**
 * A client that controls an instance of Castle.
 *
 * @author andrewbyde
 */
public interface CastleController extends CastleListener {
	
	/**
	 * Give this listener a CastleControlServer on which to operate.
	 */
	public void setServer(CastleControlServer server);
}
