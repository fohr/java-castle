package com.acunu.castle.control;

import static com.acunu.castle.control.HexWriter.hex;
import static com.acunu.castle.control.HexWriter.hexL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.acunu.castle.control.ArrayInfo.MergeState;

/**
 * In addition to DAInfo, holds cached versions of the info itself.
 */
class DAData extends DAInfo {
	private static Logger log = Logger.getLogger(DAData.class);
	
	// TODO we want a list with set-like properties -- uniqueness of elements.
	private final List<Long> arrayIds = new ArrayList<Long>();
	private final SortedSet<Long> valueExIds = new TreeSet<Long>();
	private final SortedSet<Integer> mergeIds = new TreeSet<Integer>();

	private final HashMap<Long, ArrayInfo> arrays = new HashMap<Long, ArrayInfo>();
	private final HashMap<Integer, MergeInfo> merges = new HashMap<Integer, MergeInfo>();
	private final HashMap<Long, ValueExInfo> values = new HashMap<Long, ValueExInfo>();

	public DAData(int daId) {
		super(daId);
	}

	public synchronized void clear() {
		log.debug("clear");
		arrayIds.clear();
		arrays.clear();
		mergeIds.clear();
		merges.clear();
		valueExIds.clear();
		values.clear();
	}

	public synchronized int indexOfArray(Long id) {
		return arrayIds.indexOf(id);
	}

	public synchronized boolean containsArray(Long id) {
		return arrays.containsKey(id);
	}

	public synchronized boolean containsVE(Long id) {
		return values.containsKey(id);
	}

	public synchronized boolean containsMerge(Integer id) {
		return merges.containsKey(id);
	}

	public synchronized void putArray(ArrayInfo info) {
		int loc = 0;
		// might be there are no arrays yet -- if so, loc=0 is correct.
		if (arrayIds.size() > 0) {
			int dataTime = info.dataTime;
			// might be trivially at the head of the list -- most recent.
			// if so, loc = 0 is correct
			if (dataTime <= timeOfIndex(0)) {
				int total = arrayIds.size();
				// might be trivially at the end of the list -- oldest.
				// if so, append to end.
				if (dataTime <= timeOfIndex(total - 1)) {
					if (log.isTraceEnabled()) {
						log.trace(ids + "dataTime=" + dataTime
								+ ", older than oldest, so append to back");
					}
					loc = total;
				} else {
					if (log.isTraceEnabled()) {
						log.trace(ids
								+ "dataTime in middle somewhere, so search:");
					}
					// ok, this is the case where we have to search.
					loc = glb(info.dataTime, 0, total);
				}
			} else {
				log.trace(ids + "dataTime=" + dataTime
						+ ", younger than youngest, so push to front");
			}
		} else {
			log.trace(ids + "no arrays, so push to front");
		}
		log.debug(ids + " insert " + info.ids + " at loc=" + loc);
		arrayIds.add(loc, info.id);
		arrays.put(info.id, info);
	}

	private int glb(int dataTime, int start, int fin) {
		if (start >= fin)
			return start;

		int mid = (start + fin) / 2;
		int tMid = timeOfIndex(mid);
		if (log.isTraceEnabled()) {
			log.trace(ids + "dataTime=" + dataTime + ", start=" + start
					+ ", end=" + fin + ", mid=" + mid + ", tMid=" + tMid);
		}
		if (tMid < dataTime)
			return glb(dataTime, start, mid);
		else
			return glb(dataTime, mid + 1, fin);
	}

	private int timeOfIndex(int index) {
		if (index > arrayIds.size())
			return 0;
		else if (index < 0)
			return Integer.MAX_VALUE;
		else
			return arrays.get(arrayIds.get(index)).dataTime;
	}

	public synchronized void putMerge(Integer id, MergeInfo info) {
		mergeIds.add(id);
		merges.put(id, info);
	}

	public synchronized void putValueEx(Long id, ValueExInfo info) {
		valueExIds.add(id);
		values.put(id, info);
	}

	public synchronized ArrayInfo getArray(Long id) {
		if (id == null)
			return null;
		return arrays.get(id);
	}

	public synchronized MergeInfo getMerge(Integer id) {
		if (id == null)
			return null;
		return merges.get(id);
	}

	public synchronized ValueExInfo getValueEx(Long id) {
		if (id == null)
			return null;
		return values.get(id);
	}

	public synchronized ArrayInfo removeArray(Long id) {
		arrayIds.remove(id);
		return arrays.remove(id);
	}

	public synchronized MergeInfo removeMerge(Integer id) {
		mergeIds.remove(id);
		return merges.remove(id);
	}

	public synchronized ValueExInfo removeValueEx(Long id) {
		valueExIds.remove(id);
		return values.remove(id);
	}

	public synchronized String toStringOneLine() {
		StringBuilder sb = new StringBuilder();
		List<Long> aids = new LinkedList<Long>();
		aids.addAll(arrayIds);
		sb.append("A: ");
		for (Iterator<Long> it = aids.iterator(); it.hasNext();) {
			Long aid = it.next();

			ArrayInfo info = arrays.get(aid);
			if (info == null)
				continue;
			if (info.getMergeState() == MergeState.OUTPUT)
				sb.append("+");
			sb.append(hex(info.id));
			if (info.getMergeState() == MergeState.INPUT)
				sb.append("-");

			if (it.hasNext()) {
				sb.append(", ");
			}
		}

		List<Integer> mids = new LinkedList<Integer>();
		mids.addAll(mergeIds);
		sb.append(", M: ");
		for (Iterator<Integer> it = mids.iterator(); it.hasNext();) {
			Integer mid = it.next();
			MergeInfo info = merges.get(mid);
			if (info == null)
				continue;
			List<Long> in = info.getInputArrayIds();
			List<Long> out = info.getOutputArrayIds();
			SortedSet<Long> drain = info.getExtentsToDrain();

			sb.append(hex(info.id) + "{" + hexL(in) + "->" + hexL(out)
					+ " drain" + hexL(drain) + "}");
			if (it.hasNext())
				sb.append(", ");
		}

		sb.append(", VE: " + hexL(valueExIds));
		return sb.toString();
	}

	@Override
	public synchronized List<Long> getArrayIds()
	{
		return arrayIds;
	}

	@Override
	public synchronized SortedSet<Integer> getMergeIds()
	{
		return mergeIds;
	}

	@Override
	public synchronized SortedSet<Long> getValueExIds()
	{
		return valueExIds;
	}
}