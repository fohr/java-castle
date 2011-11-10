package com.acunu.castle.control;

/**
 * A server that allows control of castle.
 * 
 * @author andrewbyde
 */
public interface CastleControlServer extends CastleView {
	/**
	 * block until Server threads exit
	 * @throws InterruptedException
	 */
	void join() throws InterruptedException;

	/**
	 * Project the control functions of this server onto a particular DA.
	 */
	DAControlServer projectControl(int daId);

	/**
	 * Register a particular controller. Only one controller can be registered.
	 */
	void setController(CastleController controller);

	/**
	 * Add a listener. There can be as many of these as you like.
	 * 
	 * @param listener
	 *            a CastleListener to add. Will be notified of interesting
	 *            events.
	 */
	void addListener(CastleListener listener);
}
