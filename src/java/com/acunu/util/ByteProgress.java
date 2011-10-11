package com.acunu.util;

import java.util.LinkedList;

/** A class which observes an accumulating statistic (measured in bytes), and 
 * at any time can give a bandwidth estimate, in bytes per second.
 * 
 * @author abyde
 */
public class ByteProgress {

	// data.
	private LinkedList<Pair<Long, Long>> data = new LinkedList<Pair<Long, Long>>();

	private Long nextTime;
	private long memory;

	/**
	 * @param memory
	 *            longest time into the past that we remember
	 */
	public ByteProgress(long memory) {
		this.memory = memory;
	}

	/**
	 * Flush everything from the list that isn't within the time window.
	 */
	private void cleanse(long t) {
		if (data.isEmpty())
			return;
		if (nextTime == null) {
			nextTime = data.peekFirst().fst();
		}

		// ok, now the real clansing
		long cutoff = t - memory;
		while ((nextTime != null) && (nextTime < cutoff)) {
			data.pollFirst();
			nextTime = (data.isEmpty()) ? null : data.peekFirst().fst();
		}
	}

	/**
	 * Add a new data point.
	 */
	public void add(long x) {
		long t = System.currentTimeMillis();
		synchronized (data) {
			data.add(new Pair<Long, Long>(t, x));
			cleanse(t);
		}
	}

	// mb/s
	private double fac = Utils.mb / 1000.0;
	
	/**
	 * Rate of the existing data, MB/s.
	 */
	public double rate() {
		cleanse(System.currentTimeMillis());
		if (data.size() < 2)
			return 0;
		Pair<Long, Long> last = data.peekLast();
		Pair<Long, Long> first = data.peekFirst();
		
		long dt = last.fst() - first.fst();
		long db = last.snd() - first.snd();
		
		return db / fac / dt;
	}
	
	public String toString() {
		String s = data.toString();
		
		if (data.size() < 2)
			return s;
		
		Pair<Long, Long> last = data.peekLast();
		Pair<Long, Long> first = data.peekFirst();
		
		long dt = last.fst() - first.fst();
		long db = last.snd() - first.snd();

		double rate = db / fac/dt;
		s += "db=" + db + " / dt=" + dt + " = " + rate;
		return s;
	}
}
