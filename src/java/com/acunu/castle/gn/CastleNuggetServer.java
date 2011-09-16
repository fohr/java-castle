package com.acunu.castle.gn;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import com.acunu.castle.Castle;
import com.acunu.castle.CastleException;

public class CastleNuggetServer implements NuggetServer {

	private Castle castleConnection;
	private GoldenNugget nugget = null;
	public String name = "nuggetServer";

	public CastleNuggetServer() throws IOException {
		this.castleConnection = new Castle(new HashMap<Integer, Integer>(),
				false);
		new CastleEventsThread(this).start();
	}

	// TODO -- implement
	public int setWriteRate(double rateMB) {
		throw new RuntimeException("not implemented yet");
	}

	/** Set target read bandwidth, MB/s. // TODO -- implement */
	public int setReadRate(double rateMB) {
		throw new RuntimeException("not implemented yet");
	}

	/** Observed write rate, MB/s. // TODO -- implement */
	public double getWriteRate() {
		throw new RuntimeException("not implemented yet");
	}

	public CastleInfo getCastleInfo() {
		File vertreesDir = new File("/sys/fs/castle-fs/vertrees");

		// map from vertree id to info for that vertree.
		Map<Integer, DAInfo> cMap = new HashMap<Integer, DAInfo>();

		File[] vertrees = vertreesDir.listFiles();
		for (int i = 0; i < vertrees.length; i++) {
			if (!vertrees[i].isDirectory())
				continue;
			int vertreeId = Integer.parseInt(vertrees[i].getName(), 16);
			cMap.put(vertreeId, getDAInfo(vertreeId, vertrees[i]));
		}

		return new CastleInfo(cMap);
	}

	/**
	 * Get the DA info for a particular vertree.
	 */
	private DAInfo getDAInfo(int vertreeId, File vertreeFile) {
		List<Integer> arrayIds = new LinkedList<Integer>();
		Set<Integer> valueExIds = new HashSet<Integer>();
		List<Integer> mergeIds = new LinkedList<Integer>();

		File arraysDir = new File(vertreeFile, "arrays");
		File[] arrays = arraysDir.listFiles();

		for (int j = 0; j < arrays.length; j++) {
			if (!arrays[j].isDirectory())
				continue;
			int arrayId = Integer.parseInt(arrays[j].getName(), 16);
			System.out.println(vertreeId + " " + vertreeFile + " " + arrays[j]
					+ " (" + vertreeId + ", " + arrayId + ")");

			arrayIds.add(arrayId);
		}

		File mergesDir = new File(vertreeFile, "merges");
		File[] merges = mergesDir.listFiles();

		for (int j = 0; j < merges.length; j++) {
			if (!merges[j].isDirectory())
				continue;
			int mergeId = Integer.parseInt(merges[j].getName(), 16);
			System.out.println(vertreeId + " " + vertreeFile + " " + merges[j]
					+ " (" + vertreeId + ", " + mergeId + ")");

			mergeIds.add(mergeId);
		}

		return new DAInfo(vertreeId, arrayIds, valueExIds, mergeIds);
	}

	public ArrayInfo getArrayInfo(int daId, int arrayId) throws CastleException {

		ArrayInfo ai = new ArrayInfo(daId, arrayId);
		try {
			String dir = "/sys/fs/castle-fs/vertrees/"
					+ Integer.toString(daId, 16) + "/arrays/"
					+ Integer.toString(arrayId, 16) + "/";
			File sizeFile = new File(dir, "size");
			BufferedReader in = new BufferedReader(new FileReader(sizeFile));
			String sizeStr = in.readLine();
			long size = Long.parseLong(sizeStr) * 1024 * 1024;
			ai.capacityInBytes = size;

			File itemsFile = new File(dir, "item_count");
			in = new BufferedReader(new FileReader(itemsFile));
			String itemsStr = in.readLine();
			long items = Long.parseLong(itemsStr);
			ai.capacityInItems = items;

		} catch (Exception e) {
			// TODO: do it properly! :)
			throw new CastleException(-22, e.toString());
		}
		// TODO: sizeInBytes & isMerging need to be dealt with

		return ai;
	}

	public MergeInfo getMergeInfo(int daId, int mergeId) throws CastleException {
		MergeInfo mi;
		List<Integer> inputArrays = new LinkedList<Integer>();
		MergeConfig config;
		List<Integer> outputArray = new LinkedList<Integer>();
		long inputItems = 0;

		try {
			String mergeDir = "/sys/fs/castle-fs/vertrees/"
					+ Integer.toString(daId, 16) + "/merges/"
					+ Integer.toString(mergeId, 16) + "/";
			/* Read off all input arrays. */
			File inTreesFile = new File(mergeDir, "in_trees");
			BufferedReader in = new BufferedReader(new FileReader(inTreesFile));
			StringTokenizer tokenizer = new StringTokenizer(in.readLine(), " ");
			while (tokenizer.hasMoreTokens()) {
				String treeIdStr = tokenizer.nextToken();
				int arrayId = Integer.parseInt(treeIdStr.substring(2), 16);
				ArrayInfo arrayInfo = this.getArrayInfo(daId, arrayId);
				inputItems += arrayInfo.capacityInItems;
			}
			config = new MergeConfig(daId, inputArrays);

			/* Read off the output array. */
			File outTreeFile = new File(mergeDir, "out_tree");
			in = new BufferedReader(new FileReader(outTreeFile));
			String treeIdStr = in.readLine();
			int arrayId = Integer.parseInt(treeIdStr.substring(2), 16);
			outputArray.add(arrayId);

			/* Construct MergeInfo. */
			mi = new MergeInfo(config, mergeId, outputArray, null);

			/* Read off progress. */
			File progressFile = new File(mergeDir, "progress");
			in = new BufferedReader(new FileReader(progressFile));
			String progressStr = in.readLine();
			mi.workDone = Long.parseLong(progressStr);
			mi.workLeft = mi.workDone > inputItems ? 0 : inputItems
					- mi.workDone;

		} catch (Exception e) {
			// TODO: do it properly! :)
			throw new CastleException(-22, e.toString());
		}

		return mi;
	}

	// TODO -- implement
	public ValueExInfo getValueExInfo(int daId, int vxid) {
		throw new RuntimeException("not implemented yet");
	}

	public MergeInfo startMerge(MergeConfig mergeConfig) throws CastleException {
		int[] arrayArray = new int[mergeConfig.inputArrayIds.size()];

		int idx = 0;
		for (int arrayId : arrayArray)
			arrayArray[idx++] = arrayId;

		/* WARNING: this hardcodes c_rda_type_t. */
		int mergeId = castleConnection.merge_start(arrayArray, 0, 0, 0);

		return getMergeInfo(mergeConfig.daId, mergeId);
	}

	private class MergeWork extends DAObject {
		public int mergeId;
		public int workId;
		public long mergeUnits;

		MergeWork(int daId, int mergeId, int workId, long mergeUnits) {
			super(daId);
			this.mergeId = mergeId;
			this.workId = workId;
			this.mergeUnits = mergeUnits;
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(super.toString());
			sb.append(t + "workId   : " + workId + "\n");
			sb.append(t + "mergeId  : " + mergeId + "\n");
			sb.append(t + "units    : " + mergeUnits + "\n");
			return sb.toString();
		}
	}

	private HashMap<Integer, MergeWork> mergeWorks = new HashMap<Integer, MergeWork>();

	/**
	 * TODO -- currently daId is unused.
	 */
	public synchronized int doWork(int daId, int mergeId, long mergeUnits)
			throws CastleException {
		int workId = castleConnection.merge_do_work(mergeId, mergeUnits);

		MergeWork work = new MergeWork(daId, mergeId, workId, mergeUnits);
		mergeWorks.put(workId, work);

		return workId;
	}

	public int mergeThreadCreate() throws CastleException {
		return castleConnection.merge_thread_create();
	}

	/**
	 * TODO -- currently daId is unused.
	 */
	public void mergeThreadAttach(int daId, int mergeId, int threadId)
			throws CastleException {
		castleConnection.merge_thread_attach(mergeId, threadId);
	}

	public void mergeThreadDestroy(int threadId) throws CastleException {
		castleConnection.merge_thread_destroy(threadId);
	}

	public synchronized void castleEvent(String s) {
		String[] args = new String[5];
		String[] prefixes = new String[] { "CMD=", "ARG1=0x", "ARG2=0x",
				"ARG3=0x", "ARG4=0x" };
		StringTokenizer tokenizer = new StringTokenizer(s, ":");
		int i;

		System.out.println("Castle Event " + s + "\n");

		/* Return if there isn't a nugget. */
		if (this.nugget == null) {
			System.out.println("No nugget registered.\n");
			return;
		}

		i = 0;
		while (tokenizer.hasMoreTokens() && (i < 5)) {
			args[i] = tokenizer.nextToken();
			if (!args[i].startsWith(prefixes[i]))
				throw new RuntimeException("Bad event string formatting: " + s);
			args[i] = args[i].substring(prefixes[i].length());
			i++;
		}

		if (i != 5)
			throw new RuntimeException("Bad event string formatting: " + s);

		if (args[0].equals("131")) {
			int arrayId = Integer.parseInt(args[2], 16);
			 int vertreeId = Integer.parseInt(args[3], 16);

			System.out.println("New array event, arrayId: " + arrayId+", vertreeId: "+vertreeId);
			// TODO: DA id needs to be part of the event
			// TODO: How to handle errors in reading the array better?
			try {
				nugget.newArray(getArrayInfo(vertreeId, arrayId));
			} catch (Exception e) {
			}
		} else if (args[0].equals("132")) {
			int workId = Integer.parseInt(args[2], 16);
			int workDone = Integer.parseInt(args[3], 16);
			int isMergeFinished = Integer.parseInt(args[4], 16);

			System.out.println("Merge work done event, workId: " + workId
					+ ", workDone: " + workDone + ", isMergeFinished: "
					+ isMergeFinished);
			MergeWork work = mergeWorks.remove(workId);
			if (work == null)
				throw new RuntimeException("Got event for non-started work");

			nugget.workDone(workId, (workDone != 0) ? work.mergeUnits : 0,
					isMergeFinished != 0);
		} else {
			throw new RuntimeException("Unknown event");
		}
	}

	public synchronized void setGoldenNugget(GoldenNugget nugget) {
		if (this.nugget != null)
			throw new RuntimeException("Cannot register nugget multiple times.");

		this.nugget = nugget;
	}

	public void terminate() throws IOException {
		castleConnection.disconnect();
	}

	public static void main(String[] args) throws Exception {
		CastleNuggetServer ns;

		System.out.println("CastleNuggetServer test ...");
		ns = new CastleNuggetServer();
		ns.getCastleInfo();
		ArrayInfo ai = ns.getArrayInfo(7, 57);
		System.out.println("Size of the array: " + ai.capacityInBytes);
		ns.terminate();
		System.out.println("... done.");
	}
}
