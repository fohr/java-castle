package com.acunu.castle.control;

import org.apache.log4j.Logger;

import com.acunu.util.Utils;

import static com.acunu.castle.control.HexWriter.*;

/**
 * Data regarding a piece of merge work conducted within Castle. An object of
 * this sort is created at the moment of submitting work to castle, and kept as
 * a record of what we believe to be happening; on completion of the work it
 * serves as a brief historical record, and has methods for inferring merge
 * speed etc.
 * 
 * @author andrewbyde
 */
public class MergeWork extends DAObject {
	private static Logger log = Logger.getLogger(MergeWork.class);
	
	/**
	 * Unique ID of this piece of work. In the current implementation, equal to
	 * merge id.
	 */
	public final int workId;

	/** "W[&lt;workId&gt;]" */
	public final String ids;

	/** Info for the merge to which this work is associated. */
	public MergeInfo mergeInfo;

	/** Units of work involved. In the current implementation, this is in bytes. */
	public long mergeUnits;

	/**
	 * Time of creation of the work -- taken to be the time at which the work is
	 * submitted to castle.
	 */
	public final long startTime = System.currentTimeMillis();

	/** Time at which the work finished. Set with 'setWorkDone'. */
	private long finishTime = -1;

	/** Number of units of work done. Set with 'setWorkDone'. */
	private long workDone = 0;

	/** Since in the current implementation workDone is in bytes, this is in MB. */
	private double workDoneMB = 0.0;

	/**
	 * Create a piece of work for the given merge, of the given size.
	 */
	MergeWork(MergeInfo info, int workId, long mergeUnits) {
		super(info.daId);
		this.mergeInfo = info;
		this.workId = workId;
		this.ids = "W[" + hex(workId) + "]";
		this.mergeUnits = mergeUnits;
	}

	/**
	 * Set that the work is done. Can only be called once.
	 * 
	 * @param workDone
	 *            the total amount of work actually done. This should almost
	 *            always be equal to the amount requested, but need not be --
	 *            for example if more was asked for than the merge warranted.
	 * @param finishTime
	 *            time at which work finished, in the usual timestamp format.
	 */
	public void setWorkDone(long workDone, long finishTime) {
		if (this.finishTime >= 0) {
			throw new IllegalArgumentException("Work " + ids + " already finished");
		}
		this.finishTime = finishTime;
		this.workDone = workDone;
		workDoneMB = workDone / Utils.mbDouble;
	}

	public long finishTime() {
		return finishTime;
	}
	
	/**
	 * Work actually done, in MB.
	 */
	public double workDoneMB() {
		if (finishTime < 0)
			return -1;
		return workDoneMB;
	}

	/**
	 * Work actually done, in bytes.
	 */
	public long workDone() {
		if (finishTime < 0)
			return -1;
		return workDone;
	}

	/**
	 * How long the work took, in ms.
	 */
	public long duration() {
		if (finishTime < 0)
			return -1;
		return finishTime - startTime;
	}

	/**
	 * Effective merge rate this represents, in mb/s.
	 */
	public double rate() {
		if (finishTime < 0)
			return -1;
		return workDoneMB * 1000.0 / (finishTime - startTime);
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append(t + "workId   : ").append(hex(workId)).append("\n");
		sb.append(t + "mergeId  : ").append(hex(mergeInfo.id)).append("\n");
		sb.append(t + "units    : ").append(mergeUnits).append("\n");
		if (finishTime > 0) {
			sb.append(t + "duration : " + duration()).append("\n");
			sb.append(t + "done     : " + Utils.toStringSize(workDone)).append(
					"\n");
			sb.append(t + "rate     : " + rate()).append("\n");
		}
		return sb.toString();
	}
}
