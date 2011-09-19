package com.acunu.castle.gn;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
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
//		System.out.println("cns :: " + s);
	}

	private Castle castleConnection;
	private GoldenNugget nugget = null;
	public String name = "nuggetServer";

	public CastleNuggetServer() throws IOException {
		this.castleConnection = new Castle(new HashMap<Integer, Integer>(),
				false);
		new CastleEventsThread(this).start();
	}

	/**
	 * Construct a file by appending the given filename to the given directory;
	 * read a line from that file; close.
	 */
	private static String readLine(File directory, String filename)
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
	 * Read an id list prefixed by the number of entries. Number is always there
	 * but might be zero.
	 */
	private static List<Integer> readQuantifiedIdList(String directory,
			String filename) throws IOException {
		String s = readLine(directory, filename);
		return quantifiedIdList(s);
	}

	/**
	 * Read an id list prefixed by the number of entries. Number is always there
	 * but might be zero.
	 */
	private static List<Integer> readQuantifiedIdList(File directory,
			String filename) throws IOException {
		String s = readLine(directory, filename);
		return quantifiedIdList(s);
	}

	/**
	 * Parse a list of ids of the form '0x12 0x123' etc.
	 */
	private static List<Integer> readIdList(String directory, String filename)
			throws IOException {
		String l = readLine(directory, filename);
		return idList(l);
	}

	/**
	 * Read an id list prefixed by the number of entries. Number is always there
	 * but might be zero.
	 * 
	 * E.g. '02 0x12 0x123'
	 * or '00'
	 */
	private static List<Integer> quantifiedIdList(String s) {
		// cut off number of entries
		int i = s.indexOf(" ");
		if (i < 0) {
			return new LinkedList<Integer>();
		} else {
			s = s.substring(i);
		}
		return idList(s);
	}
	
	/**
	 * Parse a list of ids of the form '0x12 0x123' etc.
	 */
	private static List<Integer> idList(String idListString) {
		StringTokenizer st = new StringTokenizer(idListString);
		List<Integer> l = new LinkedList<Integer>();
		while (st.hasMoreTokens()) {
			String h = st.nextToken();
			l.add(DAObject.fromHex(h.substring(2)));
		}
		return l;
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

	public CastleInfo getCastleInfo() throws IOException {
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
	private DAInfo getDAInfo(int vertreeId, File vertreeFile)
			throws IOException {
		List<Integer> arrayIds = new LinkedList<Integer>();
		Set<Integer> valueExIds = new HashSet<Integer>();
		List<Integer> mergeIds = new LinkedList<Integer>();

		// parse array information
		arrayIds = readQuantifiedIdList(vertreeFile, "array_list");

		// parse merge information
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
	 * Read the information for a single array. At present it does not report
	 * merging information correctly.  FileNotFound -> array has just been 
	 * deleted, so ignore and return null.
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

			String s = readLine(dir, "merge_state");
			ai.setMergeState(s);

			report("getArrayInfo -- done");
			return ai;

		} catch (FileNotFoundException e) {
			report("getArrayInfo -- unknown " + arrayId);
			return null;
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

		try {
			report("getMergeInfo da=" + DAObject.hex(daId) + ", merge="
					+ DAObject.hex(mergeId));

			String mergeDir = "/sys/fs/castle-fs/vertrees/"
					+ Integer.toString(daId, 16) + "/merges/"
					+ Integer.toString(mergeId, 16) + "/";
			
			// read id list
			List<Integer> inputArrays = readIdList(mergeDir, "in_trees");
			List<Integer> outputArray = readIdList(mergeDir, "out_tree");
			MergeConfig config = new MergeConfig(daId, inputArrays);
			MergeInfo mi = new MergeInfo(config, mergeId, outputArray, null);
			
			// accumulate sizes
			long maxBytes = 0;
			long currentBytes = 0;
			for(Integer id : inputArrays) {
				ArrayInfo info = getArrayInfo(daId, id);
				maxBytes += info.usedInBytes;
				maxBytes += info.currentSizeInBytes;
			}

			/* Read off progress. */
			mi.workDone = readLong(mergeDir, "progress");
			mi.workLeft = maxBytes - mi.workDone;
			report("getMergeInfo -- done");
			
			return mi;
		} catch (Exception e) {
			report("getMergeInfo -- ERROR");
			// TODO: do it properly! :)
			throw new CastleException(-22, e.toString());
		}
	}

	// TODO -- implement
	public ValueExInfo getValueExInfo(int daId, int vxid) {
		throw new RuntimeException("not implemented yet");
	}

	/**
	 * Start a merge using the given merge config.
	 * 
	 * TODO -- use medium value extents TODO -- use version stats
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
			report("doWork -- done, workId = " + workId);

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
