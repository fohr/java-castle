package com.acunu.castle.control;

/**
 * The interface that an instance of castle implements in order to be
 * controllable.
 * 
 * @author andrewbyde
 */
public interface CastleControlServer extends CastleView {

	/**
	 * Project the control functions of this server onto a particular DA.
	 */
	public DAControlServer projectControl(int daId);

	/**
	 * Register a particular controller. Only one controller can be registered.
	 */
	public void setController(CastleController controller);

	/**
	 * Add a listener. There can be as many of these as you like.
	 * 
	 * @param listener
	 *            a CastleListener to add. Will be notified of interesting
	 *            events.
	 */
	public void addListener(CastleListener listener);
}
