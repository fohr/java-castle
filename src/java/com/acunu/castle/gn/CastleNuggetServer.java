package com.acunu.castle.gn;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeSet;

import com.acunu.castle.Castle;
import com.acunu.castle.gn.ArrayInfo.MergeState;

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
	public static boolean useThreads = false;

	private Thread runThread;
	private Thread eventThread;

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
	 * Return info for the given merge. Resyncs sizes.
	 * 
	 * @see com.acunu.castle.gn.NuggetServer#getMergeInfo(int, int)
	 */
	public MergeInfo getMergeInfo(int daId, int mergeId) throws IOException {
		synchronized (syncLock) {
			DAData data = getDAData(daId);
			return getMergeInfo(data, mergeId);
		}
	}

	/**
	 * Return info for the given merge. Resyncs sizes.
	 */
	private MergeInfo getMergeInfo(DAData data, int mergeId) throws IOException {
		if (data == null)
			return null;

		synchronized(syncLock) {
			MergeInfo info = data.getMerge(mergeId);
			if (info == null)
				return null;
			
			syncMergeSizes(info);
			return info;
		}
	}
	
	/**
	 * Return info for the given value extent. Resyncs sizes.
	 * 
	 * @see com.acunu.castle.gn.NuggetServer#getValueExInfo(int, int)
	 */
	public ValueExInfo getValueExInfo(int daId, int id) throws IOException {
		synchronized (syncLock) {
			DAData data = getDAData(daId);
			return getValueExInfo(data, id);
		}
	}

	/**
	 * Return info for the given value extent. Resyncs sizes.
	 */
	private ValueExInfo getValueExInfo(DAData data, int id) throws IOException {
		if (data == null)
			return null;
		synchronized (syncLock) {
			ValueExInfo info = data.getValueEx(id);
			if (info == null)
				return null;

			// refresh sizes
			syncValueExSizes(info);

			return info;
		}
	}

	/**
	 * For a particular DA, read lists of arrays, value extents and merges from
	 * sys fs.
	 */
	private DAData fetchDAData(int daId) throws IOException {
		synchronized (syncLock) {
			DAData data = new DAData(daId);

			// parse array information. Need separate list to preserve order
			// info
			List<Integer> arrayIds = readQuantifiedIdList(data.sysFsString(),
					"array_list");
			int i = 0;
			for (Integer id : arrayIds) {
				data.putArray(id, i++, fetchArrayInfo(data, id));
			}

			File rootDir = null;
			File[] dirs = null;

			// fetch value extent information.
			// TODO sys fs hack -- should be like merges
			// rootDir = new File(data.sysFsString(), "data_extents" );
			rootDir = new File(ValueExInfo.sysFsRootString);
			dirs = rootDir.listFiles();
			for (int j = 0; j < dirs.length; j++) {
				if (!dirs[j].isDirectory())
					continue;
				int id = DAObject.fromHex(dirs[j].getName());
				data.putValueEx(id, fetchValueExInfo(data, id));
			}

			// parse merge information
			rootDir = new File(data.sysFsString(), "merges");
			dirs = rootDir.listFiles();
			for (int j = 0; j < dirs.length; j++) {
				if (!dirs[j].isDirectory())
					continue;
				int id = DAObject.fromHex(dirs[j].getName());
				MergeInfo mInfo = fetchMergeInfo(data, id);
				data.putMerge(id, mInfo);

				syncVEMergeStates(data, mInfo);
			}

			return data;
		}
	}

	/**
	 * Since merge state is not persisted for value extents, derive it from the
	 * fact that they are involved in a merge.
	 */
	private void syncVEMergeStates(DAData data, MergeInfo mInfo) {
		// fix merge state of VEs drained by the merge
		SortedSet<Integer> vIds = mInfo.extentsToDrain;
		// null means all...
		if (vIds == null) {
			vIds = getAllVEs(data, mInfo.inputArrayIds);
		}
		for (Integer vId : vIds) {
			ValueExInfo vInfo = data.getValueEx(vId);
			vInfo.mergeState = MergeState.INPUT;
		}

		// fix output, if there is one.
		Integer vId = mInfo.outputValueExtentId;
		if (vId != null) {
			ValueExInfo vInfo = data.getValueEx(vId);
			vInfo.mergeState = MergeState.OUTPUT;
		}
	}

	/**
	 * Assemble a list of all VEs attached to any of the given arrays.
	 */
	private SortedSet<Integer> getAllVEs(DAData data, List<Integer> aIds) {
		if ((aIds == null) || (data == null))
			return null;
		SortedSet<Integer> vIds = new TreeSet<Integer>();
		for (Integer aId : aIds) {
			ArrayInfo aInfo = data.getArray(aId);
			vIds.addAll(aInfo.valueExIds);
		}
		return vIds;
	}

	/**
	 * Read the information for a single array. At present it does not report
	 * merging information correctly. FileNotFound -> array has just been
	 * deleted, so ignore and return null.
	 */
	private ArrayInfo fetchArrayInfo(DAData data, int id) throws IOException {
		if (data == null)
			return null;
		synchronized (syncLock) {
			ArrayInfo ai = new ArrayInfo(data.daId, id);

			if (!ai.sysFsFile.exists())
				return null;

			// assemble size variables
			syncArraySizes(ai);

			// merge state
			String s = readLine(ai.sysFsFile, "merge_state");
			ai.setMergeState(s);

			// get list of value extents
			ai.valueExIds = readIdSet(ai.sysFsFile, "data_extents");

			return ai;
		}
	}

	/**
	 * Get the information for a single merge.
	 */
	private MergeInfo fetchMergeInfo(DAData data, int id) throws IOException {
		if (data == null)
			return null;
		synchronized (syncLock) {
			MergeInfo info = new MergeInfo(data.daId, id);
			File dir = info.sysFsFile;

			if (!dir.exists())
				return null;

			// read id list
			List<Integer> inputArrays = readIdList(dir, "in_trees");
			List<Integer> outputArray = readIdList(dir, "out_tree");

			info.inputArrayIds = inputArrays;
			info.outputArrayIds = outputArray;

			/* Read off progress. */
			syncMergeSizes(info);

			// value extent information
			List<Integer> ids = readIdList(dir, "output_data_extent");
			if (ids.isEmpty()) {
				info.outputValueExtentId = null;
			} else {
				info.outputValueExtentId = ids.get(0);
			}
			info.extentsToDrain = readIdSet(dir, "drain_list");

			return info;
		}
	}

	/**
	 * Read value ex info from sys fs.
	 */
	private ValueExInfo fetchValueExInfo(DAData data, int vxid)
			throws IOException {
		if (data == null)
			return null;
		synchronized (syncLock) {
			ValueExInfo info = new ValueExInfo(data.daId, vxid);

			if (!info.sysFsFile.exists())
				return null;

			// assemble size variables
			syncValueExSizes(info);

			// report("fetchArrayInfo -- done");
			return info;
		}
	}

	/**
	 * Keep merge size synced
	 */
	private void syncMergeSizes(MergeInfo info) throws IOException {
		if (info == null)
			return;
		if (!info.sysFsFile.exists())
			return;
		String l = readLine(info.sysFsFile, "progress");
		StringTokenizer st = new StringTokenizer(l);
		if (st.hasMoreTokens()) {
			info.workDone = Long.parseLong(st.nextToken());
		}
		if (st.hasMoreTokens()) {
			info.workTotal = Long.parseLong(st.nextToken());
		}
		// last token is percentage done -- we can calculate that
		// more precisely ourselves.
	}
	
	/**
	 * Read and keep up-to-date the size for the given array.
	 */
	private void syncArraySizes(ArrayInfo info) throws IOException {
		if (info == null)
			return;

		if (!info.sysFsFile.exists())
			return;

		try {
			// read the data...
			List<String> lines = readLines(info.sysFsFile, "size");
			if (lines.size() < 4)
				throw new RuntimeException("got " + lines.size()
						+ " lines in 'size' file");

			// 'Item Count: <count>'
			info.itemCount = Long.parseLong(lines.get(0).substring(12));

			// 'Reserved Bytes: <bytes>'
			info.reservedSizeInBytes = Long.parseLong(lines.get(1)
					.substring(16));

			// 'Used Bytes: <bytes>'
			info.usedInBytes = Long.parseLong(lines.get(2).substring(12));

			// 'Current Size Bytes: <bytes>'
			info.currentSizeInBytes = Long
					.parseLong(lines.get(3).substring(20));
		} catch (Exception e) {
			error("Could not sync array sizes for " + DAObject.hex(info.id));
			e.printStackTrace();
		}
	}

	/**
	 * Read and keep up-to-date the size for the given array.
	 */
	private void syncValueExSizes(ValueExInfo info) throws IOException {
		if (info == null)
			return;

		if (!info.sysFsFile.exists())
			return;

		// assemble size variables
		List<String> lines = readLines(info.sysFsFile, "size");

		// 'Bytes: <bytes>'
		info.sizeInBytes = Long.parseLong(lines.get(0).substring(7));

		// 'Entries: <entries>'
		info.numEntries = Long.parseLong(lines.get(1).substring(9));
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
				DAData data = getDAData(mergeConfig.daId);
				if (data == null)
					throw new IOException("Unknown da " + mergeConfig.daId);

				// convert list to array
				int[] arrayIds = new int[mergeConfig.inputArrayIds.size()];
				for (int i = 0; i < arrayIds.length; i++) {
					arrayIds[i] = mergeConfig.inputArrayIds.get(i);
				}

				// update status of input and output arrays
				// check that all arrays exist.
				for (int i = 0; i < arrayIds.length; i++) {
					int arrayId = arrayIds[i];
					if (!data.containsArray(arrayId)) {
						error("startMerge cannot use array " + arrayId);
						throw new IllegalArgumentException(
								"Cannot start a merge on non-existant array "
										+ DAObject.hex(arrayId));
					}
				}

				// assemble list of data extents to drain
				long[] dataExtentsToDrain = null;
				if (mergeConfig.extentsToDrain == null) {
					dataExtentsToDrain = null;
				} else {
					dataExtentsToDrain = new long[mergeConfig.extentsToDrain
							.size()];
					int i = 0;
					for (Integer id : mergeConfig.extentsToDrain) {
						dataExtentsToDrain[i++] = id;
					}
				}

				// where will the new array go?
				int location = data.maxIndexOfArray(mergeConfig.inputArrayIds) + 1;

				report("call castle");
				/* WARNING: this hardcodes c_rda_type_t. */
				int mergeId = castleConnection.merge_start(arrayIds,
						dataExtentsToDrain, 0, 0, 0);
				report("castle returned merge id " + mergeId);

				// this will do the fetch and cache update for the merge.
				MergeInfo mergeInfo = fetchMergeInfo(data, mergeId);
				if (mergeInfo == null) {
					throw new RuntimeException("Null merge info for merge "
							+ mergeId);
				} else {
					report("new merge info: " + mergeInfo);
				}
				data.putMerge(mergeId, mergeInfo);

				// update input arrays
				for (Integer id : mergeInfo.inputArrayIds) {
					// no size sync needed
					ArrayInfo info = data.getArray(id);
					info.mergeState = ArrayInfo.MergeState.INPUT;
				}

				// output arrays are new, so fetch and add to da data
				for (Integer id : mergeInfo.outputArrayIds) {
					ArrayInfo info = newArray(data, location, id);
					info.mergeState = ArrayInfo.MergeState.OUTPUT;
				}

				// output value extent (if any) is new,
				// so fetch and add to DA data
				Integer vId = mergeInfo.outputValueExtentId;
				if (vId != null) {
					ValueExInfo vInfo = fetchValueExInfo(data, vId);
					data.putValueEx(vId, vInfo);
				}
				syncVEMergeStates(data, mergeInfo);

				report("startMerge -- done");
				return mergeInfo;
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
			return null;
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

			if (useThreads) {
				// make sure there's a merge thread
				threadManager.ensureThreadForMerge(daId, mergeId);
			}

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
						// new T0!
						ArrayInfo info = newArray(data, 0, arrayId);
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
	 * Processes a new array event from the kernel, updating the given data with
	 * data for the array itself and for any value extents it contains.
	 */
	private ArrayInfo newArray(DAData data, int location, int id)
			throws IOException {
		synchronized (syncLock) {
			// put the new array at the head of the list.
			ArrayInfo info = fetchArrayInfo(data, id);
			data.putArray(id, location, info);
			// ensure that all these value extents are known about.
			if (info.valueExIds != null) {
				for (Integer vId : info.valueExIds) {
					newVE(data, vId);
				}
			}
			return info;
		}
	}

	/**
	 * Fetch and load into data a new value extent info with the given id. If
	 * the ValueExtent already exists in the data, then simply return it.
	 * 
	 * @return the ValueExInfo fetched.
	 */
	private ValueExInfo newVE(DAData data, Integer id) throws IOException {
		if ((data == null) || (id == null))
			return null;
		synchronized (syncLock) {
			if (data.containsVE(id))
				return data.getValueEx(id);
			ValueExInfo info = fetchValueExInfo(data, id);
			data.putValueEx(id, info);
			return info;
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

					// remove input value extents
					// TODO -- is this always correct? might be shared VEs in
					// general
					if (mergeInfo.extentsToDrain == null) {
						mergeInfo.extentsToDrain = getAllVEs(data,
								mergeInfo.inputArrayIds);
						report("null extents to drain, so set to ALL = "
								+ mergeInfo.extentsToDrain);
					}
					for (Integer id : mergeInfo.extentsToDrain) {
						data.removeValueEx(id);
					}

					// output VE, if it exists, is no longer merging
					ValueExInfo vInfo = data
							.getValueEx(mergeInfo.outputValueExtentId);
					if (vInfo != null)
						vInfo.mergeState = MergeState.NOT_MERGING;

					// sync state of output arrays
					for (Integer id : mergeInfo.outputArrayIds) {
						ArrayInfo info = getArrayInfo(data, id);
						info.mergeState = ArrayInfo.MergeState.NOT_MERGING;
					}

					// remove merge
					Integer mId = mergeInfo.id;
					data.removeMerge(mId);
					report("Removed merge " + DAObject.hex(mergeInfo.id)
							+ ", now data = " + data);

					if (useThreads) {
						// unbind
						threadManager.unbind(mergeInfo.id);
					}
				} else {
					report("event - update sizes");
					DAData data = getDAData(work.daId);
					
					// sync work done.
					syncMergeSizes(mergeInfo);
					
					// update array sizes
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
		} catch (Exception e) {
			e.printStackTrace();
			error("Could not sync after work finished: " + e.getMessage());
			throw new RuntimeException(e);
		}
		try {
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
	 * Read multiple lines from a file.
	 */
	private static List<String> readLines(File directory, String filename)
			throws IOException {
		FileReader fr = null;
		List<String> l = new LinkedList<String>();
		try {
			fr = new FileReader(new File(directory, filename));
			BufferedReader in = new BufferedReader(fr);
			String s = null;
			while ((s = in.readLine()) != null) {
				l.add(s);
			}
			return l;
		} finally {
			if (fr != null)
				fr.close();
		}
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
	 * Parse a list of ids of the form '0x12 0x123' etc.
	 */
	private static List<Integer> readIdList(File directory, String filename)
			throws IOException {
		String l = readLine(directory, filename);
		return idList(l);
	}

	/**
	 * Parse a list of ids of the form '0x12 0x123' etc.
	 */
	private static SortedSet<Integer> readIdSet(File directory, String filename)
			throws IOException {
		String l = readLine(directory, filename);
		return idSet(l);
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
		List<Integer> l = new LinkedList<Integer>();
		if (idListString == null)
			return l;
		StringTokenizer st = new StringTokenizer(idListString);
		while (st.hasMoreTokens()) {
			String h = st.nextToken();
			if (h.startsWith("0x"))
				h = h.substring(2);

			l.add(DAObject.fromHex(h));
		}
		return l;
	}

	/**
	 * Parse a list of ids of the form '0x12 0x123' etc.
	 */
	private static SortedSet<Integer> idSet(String idListString) {
		SortedSet<Integer> l = new TreeSet<Integer>();
		if (idListString == null)
			return l;
		StringTokenizer st = new StringTokenizer(idListString);
		while (st.hasMoreTokens()) {
			String h = st.nextToken();
			if (h.startsWith("0x"))
				h = h.substring(2);

			l.add(DAObject.fromHex(h));
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
	private HashMap<Integer, ArrayInfo> arrays = new HashMap<Integer, ArrayInfo>();
	private HashMap<Integer, MergeInfo> merges = new HashMap<Integer, MergeInfo>();
	private HashMap<Integer, ValueExInfo> values = new HashMap<Integer, ValueExInfo>();

	DAData(int daId) {
		super(daId, new ArrayList<Integer>(), new TreeSet<Integer>(),
				new TreeSet<Integer>());
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

	boolean containsVE(Integer id) {
		return values.containsKey(id);
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
		if (id == null)
			return null;
		return arrays.get(id);
	}

	MergeInfo getMerge(Integer id) {
		if (id == null)
			return null;
		return merges.get(id);
	}

	ValueExInfo getValueEx(Integer id) {
		if (id == null)
			return null;
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
