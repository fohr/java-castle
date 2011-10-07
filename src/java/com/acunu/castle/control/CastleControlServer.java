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
	 * Register a particular controller.
	 */
	public void setController(CastleListener controller);
}
