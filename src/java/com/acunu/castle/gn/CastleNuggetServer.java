package com.acunu.castle.gn;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import com.acunu.castle.Castle;

//import com.acunu.castle.CastleException;

/**
 * A proxy to direct events from castle to a nugget, and vice-versa from a
 * nugget down to castle.
 */
public class CastleNuggetServer implements NuggetServer, Runnable {

	private Castle castleConnection;
	private GoldenNugget nugget = null;

	private class SyncLock {
	}

	private SyncLock syncLock = new SyncLock();

	// cache!
	private long lastSoftSyncTime = 0;
	private long lastHardSyncTime = 0;
	private long softSyncDelay = 1000;
	private long hardSyncDelay = 10000;

    //	private CastleInfo castleInfo;
	private HashMap<Integer, DAData> dataMap = new HashMap<Integer, DAData>();

    private String sysFsRoot = DAObject.sysFsRoot;

    private CastleInfo makeCastleInfo() {
	synchronized(syncLock) {
	    HashMap<Integer, DAInfo> daInfoMap = new HashMap<Integer, DAInfo>();
	    for(Map.Entry<Integer, DAData> entry : dataMap.entrySet()) {
		daInfoMap.put(entry.getKey(), entry.getValue());
	    }
	    return new CastleInfo(daInfoMap);
	}
    }

	public CastleNuggetServer() throws IOException {
		this.castleConnection = new Castle(new HashMap<Integer, Integer>(),
				false);

		Thread t = new CastleEventsThread(this);
		t.setName("cns_events");
		t.start();

		Thread t2 = new Thread(this);
		t2.setName("cns_sync");
		t2.start();
	}

	/**
	 * Clear out and reconstruct all data.
	 */
	public void hardSync() {
		try {
			// report("sync");
			synchronized (syncLock) {
			    //				castleInfo = null;
				dataMap.clear();

				// this also populates the dataMap with empty
				// new DAData objects.
				fetchCastleInfo();
			}
		} catch (IOException e) {
			error("Encountered error on hard sync: " + e.getMessage());
		}
	}

	private boolean running = true;

	/**
	 * Run -- simply repeatedly syncs.
	 */
	public void run() {
		while (running) {
			long t = System.currentTimeMillis();
			if (t - lastHardSyncTime > hardSyncDelay) {
				lastHardSyncTime = t;
				lastSoftSyncTime = t;
				hardSync();
				// } else if (t - lastSoftSyncTime > softSyncDelay) {
				// lastSoftSyncTime = t;
				// softSync();
			} else {
				try {
					Thread.sleep(softSyncDelay);
				} catch (InterruptedException e) {
					e.printStackTrace();
					System.exit(1);
				}
			}
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
	 * Caching version.
	 * 
	 * @see com.acunu.castle.gn.NuggetServer#getCastleInfo()
	 */
	public CastleInfo getCastleInfo() throws IOException {
	    synchronized(syncLock) {
		if (dataMap.isEmpty())
		    fetchCastleInfo();
		return makeCastleInfo();
	    }
	}

	/**
	 * Implementation of getCastleInfo.
	 * 
	 * @throws IOException
	 */
	private void fetchCastleInfo() throws IOException {
	    synchronized(syncLock) {
		//		report("getCastleInfo");
		File vertreesDir = new File("/sys/fs/castle-fs/vertrees");
		
		// map from vertree id to info for that vertree.
		dataMap = new HashMap<Integer, DAData>();

		File[] vertrees = vertreesDir.listFiles();
		for (int i = 0; i < vertrees.length; i++) {
			if (!vertrees[i].isDirectory())
				continue;
			int vertreeId = Integer.parseInt(vertrees[i].getName(), 16);

			DAData data = fetchDAData(vertreeId);
			dataMap.put(vertreeId, data);
			//cMap.put(vertreeId, data.getDAInfo());
		}
	    }
	}

	/**
	 * Read lists of arrays, value extents and merges from sys fs.
	 */
	private DAData fetchDAData(int daId) throws IOException {
	    synchronized(syncLock) {
	    //report("fetchDAData " + daId);
		DAData data = new DAData(daId);

		// parse array information
		List<Integer> arrayIds = readQuantifiedIdList(data.sysFsString(), "array_list");
		int i = 0;
		for (Integer id : arrayIds) {
		    data.putArray(id, fetchArrayInfo(data, id));
		}

		// parse merge information
		File mergesDir = new File(data.sysFsString(), "merges");
		File[] merges = mergesDir.listFiles();
		for (int j = 0; j < merges.length; j++) {
			if (!merges[j].isDirectory())
				continue;
			Integer mergeId = Integer.parseInt(merges[j].getName(), 16);
			data.putMerge(mergeId, fetchMergeInfo(data, mergeId));
		}

		return data;
	    }
	}

	/**
	 * Caching retrieval of DA data map. If the DA is not found, do a hard sy
	 * 
	 * @throws IOException
	 *             if there is no such da, or if there is an error while
	 *             syncing.
	 */
	private DAData getDAData(int daId) throws IOException {
		DAData data = dataMap.get(daId);
		if (data == null) {
			data = fetchDAData(daId);
			dataMap.put(daId, data);
		}
		return data;
	}

	/**
	 * Caching.
	 * 
	 * @see com.acunu.castle.gn.NuggetServer#getArrayInfo(int, int)
	 */
	public ArrayInfo getArrayInfo(int daId, int arrayId) throws IOException {
	    synchronized(syncLock) {
		DAData data = getDAData(daId);
		return getArrayInfo(data, arrayId);
	    }
	}

	/**
	 * Caching.
	 * 
	 * @see com.acunu.castle.gn.NuggetServer#getArrayInfo(int, int)
	 */
	public ArrayInfo getArrayInfo(DAData data, int arrayId) throws IOException {
		assert (data != null);
		synchronized (syncLock) {
			ArrayInfo info = data.getArray(arrayId);
			if (info == null) {
				info = fetchArrayInfo(data, arrayId);
				data.putArray(arrayId, info);
			}
			return info;
		}
	}

	/**
	 * Caching.
	 * 
	 * @see com.acunu.castle.gn.NuggetServer#getMergeInfo(int, int)
	 */
	public MergeInfo getMergeInfo(int daId, int mergeId) throws IOException {
		synchronized (syncLock) {
		    DAData data = getDAData(daId);
	      
		    MergeInfo info = data.getMerge(mergeId);
			if (info == null) {
				info = fetchMergeInfo(data, mergeId);
				data.putMerge(mergeId, info);
			}
			return info;
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
			ValueExInfo info = data.getValueEx(vxId);
			if (info == null) {
				info = fetchValueExInfo(data, vxId);
				data.putValueEx(vxId, info);
			}
			return info;
		}
	}

	/**
	 * Read the information for a single array. At present it does not report
	 * merging information correctly. FileNotFound -> array has just been
	 * deleted, so ignore and return null.
	 */
	private ArrayInfo fetchArrayInfo(DAData data, int arrayId) throws IOException {
	    //		report("fetchArrayInfo da=" + DAObject.hex(data.daId) + ", arrayId=" + DAObject.hex(arrayId));

	    //		DAData data = getDAData(daId);
		ArrayInfo ai = new ArrayInfo(data.daId, arrayId);
		String dir = ai.sysFsString();
		
		File f = new File(dir);
		if (!f.exists())
			return null;

		// assemble size variables
		ai.reservedSizeInBytes = readLong(f, "reserved_size");
		ai.currentSizeInBytes = readLong(f, "current_size");
		ai.usedInBytes = readLong(f, "used");
		ai.itemCount = readLong(f, "item_count");

		// state
		String s = readLine(dir, "merge_state");
		ai.setMergeState(s);

		//report("fetchArrayInfo -- done");
		return ai;
	}

	/**
	 * Read and keep up-to-date the size for the given array.
	 */
	private void syncArraySizes(ArrayInfo arrayInfo) throws IOException {
		if (arrayInfo == null)
			return;

		File f = new File(arrayInfo.sysFsString());
		if (!f.exists())
			return;

		// assemble size variables
		arrayInfo.currentSizeInBytes = readLong(f, "current_size");
		arrayInfo.usedInBytes = readLong(f, "used");
		arrayInfo.itemCount = readLong(f, "item_count");
	}

	/**
	 * Get the information for a single merge.
	 */
	public MergeInfo fetchMergeInfo(DAData data, int mergeId) throws IOException {
	    //		report("fetchMergeInfo da=" + DAObject.hex(data.daId) + ", merge=" + DAObject.hex(mergeId));
	    //		DAData data = getDAData(daId);

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
			maxBytes += info.currentSizeInBytes;
		}

		/* Read off progress. */
		mi.workDone = readLong(mergeDir, "progress");
		mi.workLeft = maxBytes - mi.workDone;
		//report("fetchMergeInfo -- done");

		return mi;
	}

	// TODO -- implement
	private ValueExInfo fetchValueExInfo(DAData data, int vxid) throws IOException {
		throw new IOException("not implemented yet");
	}

	/**
	 * Start a merge using the given merge config. Updates array info state to
	 * the correct merge state.
	 * 
	 * TODO -- use medium value extents TODO -- use version stats
	 */
	public MergeInfo startMerge(MergeConfig mergeConfig) throws IOException {
		report("startMerge config=" + mergeConfig);

		// convert list to array
		int[] arrayIds = new int[mergeConfig.inputArrayIds.size()];
		for (int i = 0; i < arrayIds.length; i++) {
			arrayIds[i] = mergeConfig.inputArrayIds.get(i);
		}

		// update status of input and output arrays
		synchronized (syncLock) {
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
			MergeInfo mergeInfo = getMergeInfo(mergeConfig.daId, mergeId);
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
				//				ArrayInfo info = getArrayInfo(data, id);
				info.mergeState = ArrayInfo.MergeState.OUTPUT;
			    }
			}

			// for now we have to hard sync!!  This is because the ordering is tricky
			hardSync();

			report("startMerge -- done");
			return mergeInfo;
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

	private HashMap<Integer, MergeWork> mergeWorks = new HashMap<Integer, MergeWork>();

	/**
	 * TODO -- currently daId is unused.
	 */
	public synchronized int doWork(int daId, int mergeId, long mergeUnits)
			throws IOException {
	    report("doWork[" + Thread.currentThread().getName() + "] da=" + DAObject.hex(daId) + ", merge="
				+ DAObject.hex(mergeId) + ", units=" + mergeUnits);

		// fail early if there's no such merge.
		MergeInfo mergeInfo = getMergeInfo(daId, mergeId);
		int workId = castleConnection.merge_do_work(mergeId, mergeUnits);

		// construct the work
		MergeWork work = new MergeWork(mergeInfo, workId, mergeUnits);
		mergeWorks.put(workId, work);
		report("doWork -- done, workId = " + DAObject.hex(workId));

		return workId;
	}

	public int mergeThreadCreate() throws IOException {
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
	 * TODO -- currently daId is unused.
	 */
	public void mergeThreadAttach(int daId, int mergeId, int threadId)
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
	 * 
	 * @throws IOException
	 *             if the merge thread does not exist, or is attached to a
	 *             merge.
	 * @see com.acunu.castle.gn.NuggetServer#mergeThreadDestroy(int)
	 */
	public void mergeThreadDestroy(int threadId) throws IOException {
		report("mergeThreadDestroy threadId=" + DAObject.hex(threadId));
		try {
			castleConnection.merge_thread_destroy(threadId);
			report("mergeThreadDestroy -- done");
		} catch (IOException e) {
			error("mergeThreadDestroy -- ERROR");
			throw (e);
		}
	}

	/**
	 * Called by CastleEventThread when castle generates an event. In turn, the
	 * state of the cache is maintained, and the nugget is then informed.
	 */
	public synchronized void castleEvent(String s) {

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
		    synchronized(syncLock) {
		report("event " + s + "\n");

		if (args[0].equals("131")) {
			// parse "newArray" event.

			int arrayId = Integer.parseInt(args[2], 16);
			int daId = Integer.parseInt(args[3], 16);

			report("event - new array, arrayId=" + DAObject.hex(arrayId)
					+ ", vertreeId=" + DAObject.hex(daId));

			DAData data = getDAData(daId);
			// if this is the new output of a merge, then we already
			// know about it.
			if (data.containsArray(arrayId)) {
			    report("array " + DAObject.hex(arrayId) + " already known");
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
				throw new RuntimeException("Got event for non-started work");

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
			report( "null merge info");
			throw new RuntimeException("work with no merge");
		}

		// update array sizes and merge state
		try {
			synchronized (syncLock) {
				report("event - work done, workId: " + DAObject.hex(work.workId)
						+ ", workDone: " + workDone + ", merge="
						+ DAObject.hex(mergeInfo.id) + ", isMergeFinished: "
						+ isMergeFinished);
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
					report("Removed merge " + DAObject.hex(mergeInfo.id) + ", now data = " + data);
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

	/**
	 * @see com.acunu.castle.gn.NuggetServer#terminate()
	 */
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

}

class DAData extends DAInfo {
    // sysfs entry for the directory holding info on this DA.
    //    private DAInfo daInfo;
    private HashMap<Integer, ArrayInfo> arrays = new HashMap<Integer, ArrayInfo>();
    private HashMap<Integer, MergeInfo> merges = new HashMap<Integer, MergeInfo>();
    private HashMap<Integer, ValueExInfo> values = new HashMap<Integer, ValueExInfo>();
    
    DAData(int daId) {
	super(daId, new ArrayList<Integer>(), new HashSet<Integer>(), new ArrayList<Integer>());
	//	sysFs = sysFsRoot + Integer.toString(daId, 16);
    }

    int indexOfArray(Integer id) {
	return arrayIds.indexOf(id);
    }

    int maxIndexOfArray(List<Integer> ids) {
	int maxIndex = -1;
	for(Integer id : ids) {
	    maxIndex = Math.max(maxIndex, indexOfArray(id));
	}
	return maxIndex;
    }


    boolean containsArray(Integer id) {
	return arrays.containsKey(id);
    }

    /** TODO -- introduce ordering here, or we need to sync */
    void putArray(Integer id, ArrayInfo info) {
	arrayIds.add(id);
	arrays.put(id, info);
    }

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

    public String toString() {
	StringBuilder sb = new StringBuilder();
	sb.append(super.toString());
	sb.append("Maps:\n");
	sb.append("  arrays = " + DAObject.hex(arrays.keySet()) + "\n");
	sb.append("  merges = " + DAObject.hex(merges.keySet()) + "\n");
	sb.append("  values = " + DAObject.hex(values.keySet()) + "\n");
	return sb.toString();
    }
}

