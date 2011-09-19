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

/**
 * A proxy to direct events from castle to a nugget, and vice-versa from a
 * nugget down to castle.
 */
public class CastleNuggetServer implements NuggetServer {

	public static void report(String s) {
		System.out.println("cns :: " + s);
	}

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
		report("getCastleInfo");
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
			arrayIds.add(arrayId);
		}

		File mergesDir = new File(vertreeFile, "merges");
		File[] merges = mergesDir.listFiles();

		for (int j = 0; j < merges.length; j++) {
			if (!merges[j].isDirectory())
				continue;
			int mergeId = Integer.parseInt(merges[j].getName(), 16);
			mergeIds.add(mergeId);
		}

		return new DAInfo(vertreeId, arrayIds, valueExIds, mergeIds);
	}

	/**
	 * Construct a file by appending the given filename to the given directory;
	 * read a line from that file; close.
	 */
	private static String readLine(String directory, String filename)
			throws IOException {
		FileReader fr = null;
		try {
			fr = new FileReader(new File(directory, filename));
			BufferedReader in = new BufferedReader(fr);
			return in.readLine();
		} finally {
			if (fr != null)
				fr.close();
		}
	}

	/**
	 * Construct a file by appending the given filename to the given directory;
	 * read a long from that file; close.
	 */
	private static long readLong(String directory, String filename)
			throws IOException {
		return Long.parseLong(readLine(directory, filename));
	}
	
	/**
	 * Read the information for a single array.  At present it does not 
	 * report merging information correctly.
	 */
	public ArrayInfo getArrayInfo(int daId, int arrayId) throws CastleException {
		FileReader fr = null;

		ArrayInfo ai = new ArrayInfo(daId, arrayId);
		try {
			report("getArrayInfo da=" + DAObject.hex(daId) + ", arrayId="
					+ DAObject.hex(arrayId));
			String dir = "/sys/fs/castle-fs/vertrees/"
					+ Integer.toString(daId, 16) + "/arrays/"
					+ Integer.toString(arrayId, 16) + "/";

			// assemble size variables
			ai.reservedSizeInBytes = readLong(dir, "reserved_size");
			ai.currentSizeInBytes = readLong(dir, "current_size");
			ai.usedInBytes = readLong(dir, "used");
			ai.itemCount = readLong(dir, "item_count");

			// TODO -- fix up is merging.
			
			report("getArrayInfo -- done");
			return ai;

		} catch (IOException e) {
			report("getArrayInfo -- ERROR ");
			// TODO: do it properly! :)
			throw new CastleException(-22, e.toString());
		} finally {
			if (fr != null) {
				try {
					fr.close();
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(-1);
					return null;
				}
			}
		}
	}

	/**
	 * Get the information for a single merge.
	 */
	public MergeInfo getMergeInfo(int daId, int mergeId) throws CastleException {
		MergeInfo mi;
		List<Integer> inputArrays = new LinkedList<Integer>();
		MergeConfig config;
		List<Integer> outputArray = new LinkedList<Integer>();
		long inputItems = 0;

		FileReader fr = null;

		try {
			report("getMergeInfo da=" + DAObject.hex(daId) + ", merge="
					+ DAObject.hex(mergeId));

			String mergeDir = "/sys/fs/castle-fs/vertrees/"
					+ Integer.toString(daId, 16) + "/merges/"
					+ Integer.toString(mergeId, 16) + "/";
			/* Read off all input arrays. */
			File inTreesFile = new File(mergeDir, "in_trees");
			fr = new FileReader(inTreesFile);
			BufferedReader in = new BufferedReader(fr);
			String inputLine = in.readLine();

			report(" parse '" + inputLine + "'");

			StringTokenizer tokenizer = new StringTokenizer(inputLine, " ");
			while (tokenizer.hasMoreTokens()) {
				String treeIdStr = tokenizer.nextToken();
				int arrayId = Integer.parseInt(treeIdStr.substring(2), 16);
				ArrayInfo arrayInfo = this.getArrayInfo(daId, arrayId);
				inputItems += arrayInfo.usedInBytes;
				inputArrays.add(arrayId);
			}
			config = new MergeConfig(daId, inputArrays);
			fr.close();

			/* Read off the output array. */
			File outTreeFile = new File(mergeDir, "out_tree");
			fr = new FileReader(outTreeFile);
			in = new BufferedReader(fr);
			String treeIdStr = in.readLine();
			int arrayId = Integer.parseInt(treeIdStr.substring(2), 16);
			outputArray.add(arrayId);
			fr.close();

			/* Construct MergeInfo. */
			mi = new MergeInfo(config, mergeId, outputArray, null);

			/* Read off progress. */
			File progressFile = new File(mergeDir, "progress");
			fr = new FileReader(progressFile);
			in = new BufferedReader(fr);
			String progressStr = in.readLine();
			mi.workDone = Long.parseLong(progressStr);
			mi.workLeft = mi.workDone > inputItems ? 0 : inputItems
					- mi.workDone;
			fr.close();

		} catch (Exception e) {
			// TODO: do it properly! :)
			throw new CastleException(-22, e.toString());
		} finally {
			if (fr != null) {
				try {
					fr.close();
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(-1);
					return null;
				}
			}
			report("getMergeInfo -- done");
		}

		return mi;
	}

	// TODO -- implement
	public ValueExInfo getValueExInfo(int daId, int vxid) {
		throw new RuntimeException("not implemented yet");
	}

	/**
	 * Start a merge using the given merge config.
	 * 
	 * TODO -- use medium value extents
	 * TODO -- use version stats
	 */
	public MergeInfo startMerge(MergeConfig mergeConfig) throws CastleException {
		int[] arrayIds = new int[mergeConfig.inputArrayIds.size()];

		for (int i = 0; i < arrayIds.length; i++) {
			arrayIds[i] = mergeConfig.inputArrayIds.get(i);
		}

		report("startMerge arrays=" + DAObject.hex(arrayIds));

		try {
			/* WARNING: this hardcodes c_rda_type_t. */
			int mergeId = castleConnection.merge_start(arrayIds, 0, 0, 0);

			MergeInfo info = getMergeInfo(mergeConfig.daId, mergeId);
			report("startMerge -- done");
			return info;
		} catch (CastleException e) {
			report("startMerge -- ERROR " + e.getErrno());
			throw (e);
		}
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
			sb.append(t + "mergeId  : " + hex(mergeId) + "\n");
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
		report("doWork da=" + DAObject.hex(daId) + ", merge="
				+ DAObject.hex(mergeId) + ", units=" + mergeUnits);
		try {
			int workId = castleConnection.merge_do_work(mergeId, mergeUnits);

			MergeWork work = new MergeWork(daId, mergeId, workId, mergeUnits);
			mergeWorks.put(workId, work);
			report("doWork -- done");

			return workId;
		} catch (CastleException e) {
			report("doWork -- ERROR");
			throw (e);
		}
	}

	public int mergeThreadCreate() throws CastleException {
		report("mergeThreadCreate");
		try {
			int id = castleConnection.merge_thread_create();
			report("mergeThreadCreate -- done");
			return id;
		} catch (CastleException e) {
			report("mergeThreadCreate -- ERROR");
			throw (e);
		}
	}

	/**
	 * TODO -- currently daId is unused.
	 */
	public void mergeThreadAttach(int daId, int mergeId, int threadId)
			throws CastleException {
		report("mergeThreadAttach da=" + DAObject.hex(daId) + ", mergeId="
				+ DAObject.hex(mergeId) + ", threadId="
				+ DAObject.hex(threadId));
		try {
			castleConnection.merge_thread_attach(mergeId, threadId);
			report("mergeThreadAttach -- done");
		} catch (CastleException e) {
			report("mergeThreadAttach -- ERROR");
			throw (e);
		}
	}

	public void mergeThreadDestroy(int threadId) throws CastleException {
		report("mergeThreadDestroy threadId=" + DAObject.hex(threadId));
		try {
			castleConnection.merge_thread_destroy(threadId);
			report("mergeThreadDestroy -- done");
		} catch (CastleException e) {
			report("mergeThreadDestroy -- ERROR");
			throw (e);
		}
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

			report("event :: new array, arrayId=" + DAObject.hex(arrayId)
					+ ", vertreeId=" + DAObject.hex(vertreeId));
			// TODO: DA id needs to be part of the event
			// TODO: How to handle errors in reading the array better?
			try {
				nugget.newArray(getArrayInfo(vertreeId, arrayId));
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else if (args[0].equals("132")) {
			int workId = Integer.parseInt(args[2], 16);
			int workDone = Integer.parseInt(args[3], 16);
			int isMergeFinished = Integer.parseInt(args[4], 16);

			report("event :: work done, workId: " + DAObject.hex(workId)
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
		System.out.println("Size of the array: " + ai.usedInBytes);
		ns.terminate();
		System.out.println("... done.");
	}
}
