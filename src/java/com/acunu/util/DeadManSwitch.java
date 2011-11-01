package com.acunu.util;

import org.apache.log4j.Logger;

/**
 * Check every second, and System.exit if some period of time passes without a
 * heartbeat.
 */
public class DeadManSwitch implements Runnable {
	private static Logger log = Logger.getLogger(DeadManSwitch.class);
	private long lastHeartbeat;
	private long period;
	private Thread t;
	private boolean disabled = false;
	
	public DeadManSwitch(long period) {
		this.period = period;
		lastHeartbeat = System.currentTimeMillis();
	}

	public DeadManSwitch() { this(20000); }
	
	public void run() {
		if (t != null) {
			log.error("Dead man switch already running");
			return;
		}
		t = Thread.currentThread();
		
		while (!disabled && (System.currentTimeMillis() - lastHeartbeat < period)) {
			Utils.waitABit(1000);
		}
		if (!disabled) {
			log.error("Dead man switch activated");
			System.exit(1);
		}
		// else don't assume an exit at all!
	}

	public Thread start() {
		Thread t = new Thread(this);
		t.setName("dead_man");
		t.start();
		return t;
	}
	
	public void heartbeat() {
		lastHeartbeat = System.currentTimeMillis();
	}
	
	public void disable() {
		disabled = true;
		log.info("Dead man switch disabled");
	}
}
