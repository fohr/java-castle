package com.acunu.util;

import java.util.LinkedList;

import org.apache.log4j.Logger;

/**
 * A class to keep track of the progress of some bits of work. The class has a
 * time window T over which measurements are made. Work items have a start and
 * end time and a volume of work. Naively we suppose that work on each item
 * continued smoothly throughout the time window in parallel with other work
 * items.
 * 
 * The purpose of the class is to report a work rate at the current moment in
 * time. When asked to provide the current work rate, we first purge work items
 * which finished before the start of the examination time window T. We then
 * estimate that the proportion of the work in an item which was done within the
 * window T is proportional to the time overlap between T and the work item's
 * time interval.
 * 
 * @author abyde
 */
public class WorkProgressTracker {

	static class WorkItem {
		final long tStart;
		final long tEnd;
		final double size;

		private final double workPerMs;

		WorkItem(long tStart, long tEnd, double size) {
			this.tEnd = tEnd;
			tStart = Math.min(tStart, tEnd - 1);
			this.tStart = tStart;
			this.size = size;
			this.workPerMs = size / (tEnd - tStart);
		}

		/** The amount of work in this item that was done in the given interval. */
		double workInWindow(long wStart, long wEnd) {
			if ((wStart > tEnd) || (wEnd < tStart))
				return 0;
			long s = Math.max(tStart, wStart);
			long e = Math.min(tEnd, wEnd);

			return workPerMs * (e - s);
		}

		public String toString() {
			return "s:" + tStart + ", e:" + tEnd + ", w:" + size;
		}
	}

	// data.
	private LinkedList<WorkItem> data = new LinkedList<WorkItem>();

	private Long nextTime;
	private long memory;

	/** the sum of all work items seen. */
	private double totalWork = 0.0;

	/** When this tracker was created. */
	private long startTime = System.currentTimeMillis();

	/**
	 * @param memory
	 *            longest time into the past that we remember
	 */
	public WorkProgressTracker(long memory) {
		this.memory = memory;
	}

	/**
	 * Flush everything from the list that ends before t
	 */
	private void cleanse(long t) {
		synchronized (data) {
			// System.out.println("cleanse items ending before " + t);

			if (data.isEmpty())
				return;
			if (nextTime == null) {
				nextTime = data.peekFirst().tEnd;
			}

			// ok, now the real clansing
			while ((nextTime != null) && (nextTime < t)) {
				data.pollFirst();
				nextTime = (data.isEmpty()) ? null : data.peekFirst().tEnd;
			}
		}
	}

	/**
	 * Add a new data point corresponding to an instant in time, namely now.
	 */
	public void add(double work) {
		long t = System.currentTimeMillis();
		add(t - 1, t, work);
		cleanse(t - memory);
	}

	/**
	 * Add a new work item from start until now of given size.
	 */
	public void add(long start, double work) {
		long t = System.currentTimeMillis();
		add(start, t, work);
		cleanse(t - memory);
	}

	/**
	 * Add a new work item from start until end of given size. THESE MUST BE
	 * ADDED IN INCREASING ORDER OF 'end'.
	 */
	public void add(long start, long end, double work) {
		WorkItem w = new WorkItem(start, end, work);
		totalWork += work;
		synchronized (data) {
			data.add(w);
		}
	}

	/**
	 * Rate of the existing data, work/s.
	 */
	public double rate() {
		long tEnd = System.currentTimeMillis();
		long tStart = tEnd - memory;
		cleanse(tStart);
		return rate(memory);
	}

	/**
	 * Return rate over the last 'memory' milliseconds. Note that this should be
	 * smaller than the memory of the tracker itself.
	 * 
	 * @param memory
	 *            a time duration over which to measure the rate.
	 */
	public double rate(long memory) {
		if (memory <= 0)
			return 0.0;
		long tEnd = System.currentTimeMillis();
		long tStart = tEnd - memory;
		return rate(tStart, tEnd);
	}

	/**
	 * The rate over a particular time window.
	 */
	private double rate(long tStart, long tEnd) {
		long memory = tEnd - tStart;

		// add in the work for each item
		double w = 0.0;
		synchronized (data) {
			for (WorkItem wi : data) {
				double dw = wi.workInWindow(tStart, tEnd);
				w += dw;
			}
		}
		return w / memory * 1000.0;
	}

	/**
	 * The total amount of all work done divided by the lifetime of this tracker
	 * object.
	 */
	public double totalWork() {
		return totalWork;
	}

	public String toString() {
		String s = "";
		synchronized (data) {
			s = data.toString();
		}

		s += ", rate=" + rate() + ", rate(1000)=" + rate(1000);

		return s;
	}
}
