package com.acunu.castle.control;

import static com.acunu.castle.control.HexWriter.hex;
import static com.acunu.castle.control.HexWriter.hexL;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.acunu.castle.control.ArrayInfo.MergeState;
import com.acunu.util.Function;
import com.acunu.util.Functional;

/**
 * In addition to DAInfo, holds cached versions of the info itself.
 */
class DAData extends DAInfo {
	private static Logger log = Logger.getLogger(DAData.class);
	
	private final Function<ArrayInfo, Long> getId = new Function<ArrayInfo, Long>(){
		@Override
		public Long evaluate(ArrayInfo x)
		{
			return x.id;
		}
	};

	private final Comparator<ArrayInfo> dataTimeCmp = new Comparator<ArrayInfo>(){
		@Override
		public int compare(ArrayInfo a, ArrayInfo b)
		{
			return a.dataTime < b.dataTime ? 1 :
				a.dataTime > b.dataTime ? -1 : 0;
		}
	};

	private final HashMap<Long, ArrayInfo> arrays = new HashMap<Long, ArrayInfo>();
	private final SortedMap<Integer, MergeInfo> merges = new TreeMap<Integer, MergeInfo>();
	private final SortedMap<Long, ValueExInfo> values = new TreeMap<Long, ValueExInfo>();

	public DAData(int daId) {
		super(daId);
	}
	
	public DAData(DAData other) {
		super(other.daId);
		synchronized(other) {
			arrays.putAll(other.arrays);
			merges.putAll(other.merges);
			values.putAll(other.values);
		}
	}

	public synchronized void clear() {
		log.debug("clear");
		arrays.clear();
		merges.clear();
		values.clear();
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
		arrays.put(info.id, info);
	}

	public synchronized void putMerge(Integer id, MergeInfo info) {
		merges.put(id, info);
	}

	public synchronized void putValueEx(Long id, ValueExInfo info) {
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
		return arrays.remove(id);
	}

	public synchronized MergeInfo removeMerge(Integer id) {
		return merges.remove(id);
	}

	public synchronized ValueExInfo removeValueEx(Long id) {
		return values.remove(id);
	}

	@Override
	public synchronized String toStringOneLine() {
		StringBuilder sb = new StringBuilder();
		List<Long> aids = getArrayIds();
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
		mids.addAll(getMergeIds());
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

		sb.append(", VE: " + hexL(getValueExIds()));
		return sb.toString();
	}

	@Override
	public synchronized List<Long> getArrayIds()
	{
		// TODO: to avoid sorting every time we need a MultiSet (eg. from apache commons or google collections)
		// that is also a SortedSet
		List<ArrayInfo> infos = new ArrayList<ArrayInfo>(arrays.values());
		Collections.sort(infos, dataTimeCmp);
		return Collections.unmodifiableList(new ArrayList<Long>(Functional.map(infos, getId)));
	}

	@Override
	public synchronized List<Integer> getMergeIds()
	{
		return Collections.unmodifiableList(new ArrayList<Integer>(merges.keySet()));
	}

	@Override
	public synchronized List<Long> getValueExIds()
	{
		return Collections.unmodifiableList(new ArrayList<Long>(values.keySet()));
	}
}