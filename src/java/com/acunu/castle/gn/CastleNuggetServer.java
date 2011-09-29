package com.acunu.castle.gn;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import com.acunu.castle.Castle;

/**
 * A proxy to direct events from castle to a nugget, and vice-versa from a
 * nugget down to castle. This proxy maintains a consistent view of the state of
 * the server using a global lock 'syncLock'; any thread performing calculations
 * on the basis of the state of the server should lock using this sync lock
 * first.
 */
public class CastleNuggetServer implements NuggetServer, Runnable {

	/**
	 * Server. Used by the control actions 'startMerge' and 'doWork'; generates
	 * events that call the 'castleEvent' method on this class.
	 */
	private Castle castleConnection;

	/**
	 * Client. Pass server events to the nugget after synchronizing state
	 * information, and likewise pass control messages down then synchronize
	 * state information.
	 */
	private GoldenNugget nugget = null;

	/**
	 * A class for naming the sync lock. Use a separate lock to enable better
	 * debugging with tools such as jconsole.
	 */
	public static class SyncLock {
	}

	/**
	 * The global sync lock that will be held whenever changes to the state are
	 * being enacted.
	 */
	public static final SyncLock syncLock = new SyncLock();

	// used to sync at least once in a while.
	private long lastRefreshTime = 0;

	/**
	 * Occasionally we refresh our view of the server from scratch by re-reading
	 * all data.
	 */
	private long refreshDelay = 60000;

	/**
	 * For each DA, a collection of data.
	 */
	private final CastleData castleData = new CastleData();

	private final MergeThreadManager threadManager;
	private Thread runThread;
	private Thread eventThread;

	/** List of on-going work, indexed by work id. */
	private HashMap<Integer, MergeWork> mergeWorks = new HashMap<Integer, MergeWork>();

	/**
	 * Constructor, in which we attempt to bind to the server. Two threads are
	 * spawned -- one 'cns_events' which handles castle events using a
	 * CastleEventsThread object; the other 'cns_sync' which regularly refreshes
	 * the CNS's view of the server by calling the refresh method.
	 * 
	 * @see #refresh()
	 * @throws IOException
	 *             if a connection to castle cannot be established.
	 */
	public CastleNuggetServer() throws IOException {
		report("---- create ----");
		this.castleConnection = new Castle(new HashMap<Integer, Integer>(),
				false);

		threadManager = new MergeThreadManager(this);

		eventThread = new CastleEventsThread(this);
		eventThread.setName("cns_events");
		eventThread.start();

		runThread = new Thread(this);
		runThread.setName("cns_sync");
		runThread.start();
	}

	/**
	 * Boolean to control the refresh thread. When set to false the thread will
	 * exit.
	 */
	private boolean running = true;

	/**
	 * Run -- simply repeatedly refreshes.
	 */
	public void run() {
		while (running) {
			long t = System.currentTimeMillis();
			if (t - lastRefreshTime > refreshDelay) {
				refresh();
			} else {
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
	}

	/**
	 * @see com.acunu.castle.gn.NuggetServer#terminate()
	 */
	public void terminate() throws IOException {
		try {
			running = false;
			report("Wait for run thread to exit");
			runThread.join();

			// TODO -- join events thread as well
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		castleConnection.disconnect();
	}

	/**
	 * Read all information for the system into the 'dataMap' store of
	 * information. Scans the sysFs root for directories, and interprets each as
	 * a store of DA information for the DA with id equal to the directory name.
	 */
	public void refresh() {
		lastRefreshTime = System.currentTimeMillis();
		try {
			report("---- refresh ----");
			synchronized (syncLock) {
				castleData.clear();

				File vertreesDir = new File(DAObject.sysFsRoot);
				File[] vertrees = vertreesDir.listFiles();
				for (int i = 0; i < vertrees.length; i++) {
					if (!vertrees[i].isDirectory())
						continue;
					int daId = DAObject.fromHex(vertrees[i].getName());

					DAData data = fetchDAData(daId);
					castleData.put(daId, data);
				}
			}
		} catch (IOException e) {
			error("Encountered error on hard sync: " + e.getMessage());
		}
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

	/**
	 * Return a high-level description of the arrays, merges and value extents
	 * that exist.
	 * 
	 * @see com.acunu.castle.gn.NuggetServer#getCastleInfo()
	 */
	public CastleInfo getCastleInfo() throws IOException {
		synchronized (syncLock) {
			return castleData;
		}
	}

	/**
	 * Read lists of arrays, value extents and merges from sys fs.
	 */
	private DAData fetchDAData(int daId) throws IOException {
		synchronized (syncLock) {
			// report("fetchDAData " + daId);
			DAData data = new DAData(daId);

			// parse array information
			List<Integer> arrayIds = readQuantifiedIdList(data.sysFsString(),
					"array_list");
			int i = 0;
			for (Integer id : arrayIds) {
				data.putArray(id, i++, fetchArrayInfo(data, id));
			}

			// parse merge information
			File mergesDir = new File(data.sysFsString(), "merges");
			File[] merges = mergesDir.listFiles();
			for (int j = 0; j < merges.length; j++) {
				if (!merges[j].isDirectory())
					continue;
				int mergeId = DAObject.fromHex(merges[j].getName());
				// Integer mergeId = Integer.parseInt(merges[j].getName(), 16);
				data.putMerge(mergeId, fetchMergeInfo(data, mergeId));
			}

			// TODO -- fetch value extent information.

			return data;
		}
	}

	/**
	 * Caching retrieval of DA data map. If the DA is not found, refresh. If
	 * it's still not found, return null. This is called by many inspection
	 * methods in this class in order to ensure that the relevant DA exists and
	 * is up-to-date.
	 * 
	 * @return null if there is no such DA.
	 * @throws IOException
	 *             if there is an error while refreshing.
	 */
	private DAData getDAData(int daId) throws IOException {
		synchronized (syncLock) {
			DAData data = castleData.getData(daId);

			// if the da is not there then maybe we're out of sync?
			if (data == null) {
				refresh();
				data = castleData.getData(daId);
			}

			// if it's still null then it doesn't exist.
			return data;
		}
	}

	/**
	 * Return information for the given array.
	 * 
	 * @see com.acunu.castle.gn.NuggetServer#getArrayInfo(int, int)
	 */
	public ArrayInfo getArrayInfo(int daId, int arrayId) throws IOException {
		synchronized (syncLock) {
			DAData data = getDAData(daId);
			return getArrayInfo(data, arrayId);
		}
	}

	/**
	 * Return information for the given array. If the array is not found, return
	 * null. Otherwise, before returning the array info, refresh the size and
	 * state information with 'syncArrayInfo', since this information changes
	 * frequently.
	 * 
	 * @return null if the da data is null or the array does not exist.
	 */
	private ArrayInfo getArrayInfo(DAData data, int arrayId) throws IOException {
		if (data == null)
			return null;
		synchronized (syncLock) {
			ArrayInfo info = data.getArray(arrayId);
			if (info == null)
				return null;

			syncArraySizes(info);
			return info;
		}
	}

	/**
	 * Return info for the given merge. Does not resync anything.
	 * 
	 * @see com.acunu.castle.gn.NuggetServer#getMergeInfo(int, int)
	 */
	public MergeInfo getMergeInfo(int daId, int mergeId) throws IOException {
		synchronized (syncLock) {
			DAData data = getDAData(daId);
			if (data == null)
				return null;

			return data.getMerge(mergeId);
		}
	}

	/**
	 * Caching.
	 * 
	 * @see com.acunu.castle.gn.NuggetServer#getValueExInfo(int, int)
	 */
	public ValueExInfo getValueExInfo(int daId, int vxId) throws IOException {
		synchronized (syncLock) {
			DAData data = getDAData(daId);
			if (data == null)
				return null;

			ValueExInfo info = data.getValueEx(vxId);
			if (info == null)
				return null;

			// refresh sizes
			syncVESizes(info);

			return info;
		}
	}

	/**
	 * Read and keep up-to-date the size for the given array.
	 */
	private void syncVESizes(ValueExInfo info) throws IOException {
		if (info == null)
			return;

		if (!info.sysFsFile.exists())
			return;

		// assemble size variables
		// info.currentSizeInBytes = readLong(arrayInfo.sysFsFile,
		// "current_size");
	}

	/**
	 * Read the information for a single array. At present it does not report
	 * merging information correctly. FileNotFound -> array has just been
	 * deleted, so ignore and return null.
	 */
	private ArrayInfo fetchArrayInfo(DAData data, int arrayId)
			throws IOException {
		if (data == null)
			return null;
		synchronized (syncLock) {
			// report("fetchArrayInfo da=" + DAObject.hex(data.daId) +
			// ", arrayId="
			// + DAObject.hex(arrayId));

			ArrayInfo ai = new ArrayInfo(data.daId, arrayId);

			if (!ai.sysFsFile.exists())
				return null;

			// load information from the file
			ai.reservedSizeInBytes = readLong(ai.sysFsFile, "reserved_size");
			// state
			String s = readLine(ai.sysFsFile, "merge_state");
			ai.setMergeState(s);

			// assemble size variables

			syncArraySizes(ai);
			// report("fetchArrayInfo -- done");
			return ai;
		}
	}

	/**
	 * Read and keep up-to-date the size for the given array.
	 */
	private void syncArraySizes(ArrayInfo arrayInfo) throws IOException {
		if (arrayInfo == null)
			return;

		if (!arrayInfo.sysFsFile.exists())
			return;

		// assemble size variables
		arrayInfo.currentSizeInBytes = readLong(arrayInfo.sysFsFile,
				"current_size");
		arrayInfo.usedInBytes = readLong(arrayInfo.sysFsFile, "used");
		arrayInfo.itemCount = readLong(arrayInfo.sysFsFile, "item_count");
	}

	/**
	 * Get the information for a single merge.
	 */
	private MergeInfo fetchMergeInfo(DAData data, int mergeId)
			throws IOException {
		// report("fetchMergeInfo da=" + DAObject.hex(data.daId) + ", merge=" +
		// DAObject.hex(mergeId));
		// DAData data = getDAData(daId);

		String mergeDir = data.sysFsString() + "/merges/"
				+ Integer.toString(mergeId, 16) + "/";
		File f = new File(mergeDir);
		if (!f.exists())
			return null;

		// read id list
		List<Integer> inputArrays = readIdList(mergeDir, "in_trees");
		List<Integer> outputArray = readIdList(mergeDir, "out_tree");
		MergeConfig config = new MergeConfig(data.daId, inputArrays);
		MergeInfo mi = new MergeInfo(config, mergeId, outputArray, null);

		// accumulate sizes
		long maxBytes = 0;
		long currentBytes = 0;
		for (Integer id : inputArrays) {
			// note this uses cached data where available
			ArrayInfo info = getArrayInfo(data, id);
			maxBytes += info.usedInBytes;
			currentBytes += info.currentSizeInBytes;
		}

		/* Read off progress. */
		mi.workDone = readLong(mergeDir, "progress");
		mi.workLeft = maxBytes - mi.workDone;
		// report("fetchMergeInfo -- done");

		return mi;
	}

	// TODO -- implement
	private ValueExInfo fetchValueExInfo(DAData data, int vxid)
			throws IOException {
		throw new IOException("not implemented yet");
	}

	/**
	 * Start a merge using the given merge config. Updates array info state to
	 * the correct merge state.
	 * 
	 * TODO -- use medium value extents TODO -- use version stats
	 */
	public MergeInfo startMerge(MergeConfig mergeConfig) throws IOException {
		try {
			synchronized (syncLock) {
				report("startMerge config=" + mergeConfig);

				// convert list to array
				int[] arrayIds = new int[mergeConfig.inputArrayIds.size()];
				for (int i = 0; i < arrayIds.length; i++) {
					arrayIds[i] = mergeConfig.inputArrayIds.get(i);
				}

				// update status of input and output arrays
				// check that all arrays exist.
				DAData data = getDAData(mergeConfig.daId);
				for (int i = 0; i < arrayIds.length; i++) {
					int arrayId = arrayIds[i];
					if (!data.containsArray(arrayId)) {
						error("startMerge cannot use array " + arrayId);
						throw new RuntimeException(
								"Cannot start a merge on non-existant array "
										+ arrayId);
					}
				}

				int location = data.maxIndexOfArray(mergeConfig.inputArrayIds) + 1;

				/* WARNING: this hardcodes c_rda_type_t. */
				int mergeId = castleConnection.merge_start(arrayIds, 0, 0, 0);

				// this will do the fetch and cache update for the merge.
				MergeInfo mergeInfo = fetchMergeInfo(data, mergeId);
				if (mergeInfo == null) {
					throw new RuntimeException("Null merge info for merge "
							+ mergeId);
				} else {
					report("new merge info: " + mergeInfo);
				}
				data.putMerge(mergeId, mergeInfo);

				// and update arrays
				for (Integer id : mergeInfo.inputArrayIds) {
					ArrayInfo info = getArrayInfo(data, id);
					info.mergeState = ArrayInfo.MergeState.INPUT;
				}
				for (Integer id : mergeInfo.outputArrayIds) {
					ArrayInfo info = fetchArrayInfo(data, id);
					if (info == null) {
						throw new RuntimeException("NULL NEW ARRAY INFO");
					} else {
						data.putArray(id, location, info);
						// ArrayInfo info = getArrayInfo(data, id);
						info.mergeState = ArrayInfo.MergeState.OUTPUT;
					}
				}

				// for now we have to hard sync!! This is because the ordering
				// is
				// tricky
				// refresh();

				report("startMerge -- done");
				return mergeInfo;
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
			return null;
		}
	}

	private class MergeWork extends DAObject {
		public MergeInfo mergeInfo;
		public int workId;
		public long mergeUnits;

		MergeWork(MergeInfo info, int workId, long mergeUnits) {
			super(info.daId);
			this.mergeInfo = info;
			this.workId = workId;
			this.mergeUnits = mergeUnits;
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(super.toString());
			sb.append(t + "workId   : " + hex(workId) + "\n");
			sb.append(t + "mergeId  : " + hex(mergeInfo.id) + "\n");
			sb.append(t + "units    : " + mergeUnits + "\n");
			return sb.toString();
		}
	}

	/**
	 * TODO -- currently daId is unused.
	 */
	public int doWork(int daId, int mergeId, long mergeUnits)
			throws IOException {
		synchronized (syncLock) {
			report("doWork[" + Thread.currentThread().getName() + "] da="
					+ DAObject.hex(daId) + ", merge=" + DAObject.hex(mergeId)
					+ ", units=" + mergeUnits);

			// fail early if there's no such merge.
			MergeInfo mergeInfo = getMergeInfo(daId, mergeId);

			// make sure there's a merge thread
			threadManager.ensureThreadForMerge(daId, mergeId);

			// submit the work
			int workId = castleConnection.merge_do_work(mergeId, mergeUnits);

			// construct the work
			MergeWork work = new MergeWork(mergeInfo, workId, mergeUnits);
			mergeWorks.put(workId, work);
			report("doWork -- done, workId = " + DAObject.hex(workId));

			return workId;
		}
	}

	/**
	 * Called by CastleEventThread when castle generates an event. In turn, the
	 * state of the cache is maintained, and the nugget is then informed.
	 */
	public void castleEvent(String s) {

		String[] args = new String[5];
		String[] prefixes = new String[] { "CMD=", "ARG1=0x", "ARG2=0x",
				"ARG3=0x", "ARG4=0x" };
		StringTokenizer tokenizer = new StringTokenizer(s, ":");
		int i;

		/* Return if there isn't a nugget. */
		if (this.nugget == null) {
			report("event - No nugget registered.\n");
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

		try {
			synchronized (syncLock) {
				report("event " + s + "\n");

				if (args[0].equals("131")) {
					// parse "newArray" event.

					int arrayId = Integer.parseInt(args[2], 16);
					int daId = Integer.parseInt(args[3], 16);

					report("event - new array, arrayId="
							+ DAObject.hex(arrayId) + ", vertreeId="
							+ DAObject.hex(daId));

					DAData data = getDAData(daId);
					// if this is the new output of a merge, then we already
					// know about it.
					if (data.containsArray(arrayId)) {
						report("array " + DAObject.hex(arrayId)
								+ " already known");
					} else {
						// put the new array at the head of the list.
						ArrayInfo info = fetchArrayInfo(data, arrayId);
						data.putArray(arrayId, 0, info);
						nugget.newArray(info);
					}
				} else if (args[0].equals("132")) {
					// parse "workDone" event.

					Integer workId = Integer.parseInt(args[2], 16);
					int workDone = Integer.parseInt(args[3], 16);
					int isMergeFinished = Integer.parseInt(args[4], 16);

					MergeWork work = mergeWorks.remove(workId);
					if (work == null)
						throw new RuntimeException(
								"Got event for non-started work");

					// do the cache maintenance work in the following method.
					workDone(work, workDone, isMergeFinished != 0);
				} else {
					throw new RuntimeException("Unknown event");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Keep internal state correct, and propagate workDone event to nugget.
	 */
	private void workDone(MergeWork work, long workDone, boolean isMergeFinished) {
		MergeInfo mergeInfo = work.mergeInfo;
		if (mergeInfo == null) {
			report("null merge info");
			throw new RuntimeException("work with no merge");
		}

		// update array sizes and merge state
		try {
			synchronized (syncLock) {
				report("event - work done, workId: "
						+ DAObject.hex(work.workId) + ", workDone: " + workDone
						+ ", merge=" + DAObject.hex(mergeInfo.id)
						+ ", isMergeFinished: " + isMergeFinished);
				// if finished, remove the merge and input arrays from the
				// cache pre-emptively
				if (isMergeFinished) {
					report("event - sync merge finished");
					DAData data = getDAData(work.daId);

					// remove arrays
					for (Integer id : mergeInfo.inputArrayIds) {
						data.removeArray(id);
					}
					for (Integer id : mergeInfo.outputArrayIds) {
						ArrayInfo info = getArrayInfo(data, id);
						info.mergeState = ArrayInfo.MergeState.NOT_MERGING;
					}

					// remove merge
					Integer mId = mergeInfo.id;
					data.removeMerge(mId);
					report("Removed merge " + DAObject.hex(mergeInfo.id)
							+ ", now data = " + data);

					// unbind thread
					threadManager.unbind(mergeInfo.id);

				} else {
					report("event - update sizes");
					// update sizes
					DAData data = getDAData(work.daId);
					mergeInfo.workDone += workDone;
					mergeInfo.workLeft = Math.max(0, mergeInfo.workLeft
							- workDone);
					for (Integer id : mergeInfo.inputArrayIds) {
						ArrayInfo info = getArrayInfo(data, id);
						syncArraySizes(info);
					}
					for (Integer id : mergeInfo.outputArrayIds) {
						ArrayInfo info = getArrayInfo(data, id);
						syncArraySizes(info);
					}
					// TODO sync merge size
					// TODO sync VE sizes for those being drained and for new
					// one
				}
			}
			report("event - data synced, now call nugget");
			nugget.workDone(work.workId, (workDone != 0) ? work.mergeUnits : 0,
					isMergeFinished);
			report("event - nugget called");
		} catch (Exception e) {
			e.printStackTrace();
			error("Could not sync after work finished: " + e.getMessage());
			throw new RuntimeException(e);
		}
	}

	/**
	 * @see com.acunu.castle.gn.NuggetServer#setGoldenNugget(com.acunu.castle.gn.GoldenNugget)
	 */
	public synchronized void setGoldenNugget(GoldenNugget nugget) {
		if (this.nugget != null)
			throw new RuntimeException("Cannot register nugget multiple times.");

		this.nugget = nugget;
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
	 * Construct a file by appending the given filename to the given directory;
	 * read a long from that file; close.
	 */
	private static long readLong(File directory, String filename)
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
	 * E.g. '02 0x12 0x123' or '00'
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

	public static void report(String s) {
		System.out.println("cns :: " + s);
	}

	public static void error(String s) {
		System.out.println("cns ERROR :: " + s);
	}

	/**
	 * Create a merge thread.
	 */
	int mergeThreadCreate() throws IOException {
		report("mergeThreadCreate");
		try {
			int id = castleConnection.merge_thread_create();
			report("mergeThreadCreate -- done");
			return id;
		} catch (IOException e) {
			error("mergeThreadCreate -- ERROR");
			throw (e);
		}
	}

	/**
	 * attach a merge thread to a merge.
	 */
	void mergeThreadAttach(int daId, int mergeId, int threadId)
			throws IOException {
		report("mergeThreadAttach da=" + DAObject.hex(daId) + ", mergeId="
				+ DAObject.hex(mergeId) + ", threadId="
				+ DAObject.hex(threadId));
		try {
			castleConnection.merge_thread_attach(mergeId, threadId);
			report("mergeThreadAttach -- done");
		} catch (IOException e) {
			error("mergeThreadAttach -- ERROR");
			throw (e);
		}
	}

	/**
	 * Destroy a merge thread.
	 */
	void mergeThreadDestroy(int threadId) throws IOException {
		report("mergeThreadDestroy threadId=" + DAObject.hex(threadId));
		try {
			castleConnection.merge_thread_destroy(threadId);
			report("mergeThreadDestroy -- done");
		} catch (IOException e) {
			error("mergeThreadDestroy -- ERROR");
			throw (e);
		}
	}

}

/**
 * Castle data holds more than castle info, because DAData holds more than
 * DAInfo.
 */
class CastleData extends CastleInfo {
	final private HashMap<Integer, DAData> dataMap = new HashMap<Integer, DAData>();

	public void clear() {
		super.clear();
		dataMap.clear();
	}

	public void put(int daId, DAData data) {
		daIds.add(daId);
		dataMap.put(daId, data);
	}

	public DAInfo getInfo(int daId) {
		return dataMap.get(daId);
	}

	public DAData getData(int daId) {
		return dataMap.get(daId);
	}
}

/**
 * In addition to DAInfo, holds cached versions of the info itself.
 */
class DAData extends DAInfo {
	// sysfs entry for the directory holding info on this DA.
	// private DAInfo daInfo;
	private HashMap<Integer, ArrayInfo> arrays = new HashMap<Integer, ArrayInfo>();
	private HashMap<Integer, MergeInfo> merges = new HashMap<Integer, MergeInfo>();
	private HashMap<Integer, ValueExInfo> values = new HashMap<Integer, ValueExInfo>();

	DAData(int daId) {
		super(daId, new ArrayList<Integer>(), new HashSet<Integer>(),
				new ArrayList<Integer>());
		// sysFs = sysFsRoot + Integer.toString(daId, 16);
	}

	int indexOfArray(Integer id) {
		return arrayIds.indexOf(id);
	}

	int maxIndexOfArray(List<Integer> ids) {
		int maxIndex = -1;
		for (Integer id : ids) {
			maxIndex = Math.max(maxIndex, indexOfArray(id));
		}
		return maxIndex;
	}

	boolean containsArray(Integer id) {
		return arrays.containsKey(id);
	}

	/**
	 * Note that we need to know where it is.
	 */
	void putArray(Integer id, int location, ArrayInfo info) {
		arrayIds.add(location, id);
		arrays.put(id, info);
	}

	void putMerge(Integer id, MergeInfo info) {
		mergeIds.add(id);
		merges.put(id, info);
	}

	void putValueEx(Integer id, ValueExInfo info) {
		valueExIds.add(id);
		values.put(id, info);
	}

	ArrayInfo getArray(Integer id) {
		return arrays.get(id);
	}

	MergeInfo getMerge(Integer id) {
		return merges.get(id);
	}

	ValueExInfo getValueEx(Integer id) {
		return values.get(id);
	}

	ArrayInfo removeArray(Integer id) {
		arrayIds.remove(id);
		return arrays.remove(id);
	}

	MergeInfo removeMerge(Integer id) {
		mergeIds.remove(id);
		return merges.remove(id);
	}

	ValueExInfo removeValueEx(Integer id) {
		valueExIds.remove(id);
		return values.remove(id);
	}
}
