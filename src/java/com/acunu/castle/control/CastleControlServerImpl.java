package com.acunu.castle.control;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.acunu.castle.Castle;
import com.acunu.castle.CastleException;
import com.acunu.castle.control.ArrayInfo.MergeState;
import com.acunu.util.Properties;
import com.acunu.util.Utils;
import com.acunu.util.WorkProgressTracker;

/**
 * A proxy to direct events from castle to a nugget, and vice-versa from a
 * nugget down to castle. This proxy maintains a consistent view of the state of
 * the server using a global lock 'syncLock'; any thread performing calculations
 * on the basis of the state of the server should lock using this sync lock
 * first.
 */
public class CastleControlServerImpl extends HexWriter implements
		CastleControlServer, Runnable {
	private static Logger log = Logger.getLogger(CastleControlServerImpl.class);

	/**
	 * Server. Used by the control actions 'startMerge' and 'doWork'; generates
	 * events that call the 'castleEvent' method on this class.
	 */
	private static Castle castleConnection;
	private static Thread eventThread;

	/**
	 * Client. Pass server events to the nugget after synchronizing state
	 * information, and likewise pass control messages down then synchronize
	 * state information.
	 */
	private CastleController controller = null;
	private List<CastleListener> listeners = new ArrayList<CastleListener>();

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
	private long lastWriteTime = 0;

	/**
	 * Occasionally we refresh our view of the server from scratch by re-reading
	 * all data.
	 */
	private long refreshDelay = 60000;

	// for rate measurements
	private long shortTimeInterval = 1000l;
	// for rate measurements
	private long longTimeInterval = 10000l;

	// refresh the write rate estimates three times per second.
	private long writeRateDelay = 300;

	// heartbeat once per second
	private long heartbeatDelay = 1000l;

	private TreeMap<Integer, DAControlServerImpl> projections = new TreeMap<Integer, DAControlServerImpl>();

	private Thread runThread;

	/** List of on-going work, indexed by work id. */
	private HashMap<Integer, MergeWork> mergeWorks = new HashMap<Integer, MergeWork>();

	/** If true, sync errors cause failure. */
	private boolean failOnError;

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
	public CastleControlServerImpl(Properties props) throws IOException {
		log.info("---- create ----");

		failOnError = props.getBoolean("gn.failOnError", false);

		castleConnection = new Castle(new HashMap<Integer, Integer>(), false);
		eventThread = new CastleEventsThread(this);
		eventThread.setName("castle_evt");
		eventThread.start();

		runThread = new Thread(this);
		runThread.setName("ctrl_sync");
		runThread.start();
	}

	/**
	 * Boolean to control the refresh thread. When set to false the thread will
	 * exit.
	 */
	private boolean running = true;
	private long lastHeartbeatTime = 0l;

	/**
	 * Run. Regularly refreshes the write rate, and periodically re-reads all
	 * data.
	 */
	public void run() {

		// register.
		log.info("Register nugget");
		try {
			castleConnection.castle_register();
		} catch (Exception e) {
			log.warn("Error registering: " + e.getMessage());
			running = false;
		}

		while (running) {
			long t = System.currentTimeMillis();

			if (t - lastHeartbeatTime > heartbeatDelay) {
				lastHeartbeatTime = t;
				try {
					castleConnection.castle_heartbeat();
				} catch (Exception e) {
					log.error("Heartbeat failed: " + e.getMessage());
					running = false;
					continue;
				}
			}

			if (t - lastWriteTime > writeRateDelay) {
				// refresh all the write measurements
				lastWriteTime = t;
				for (DAControlServerImpl s : projections.values()) {
					s.refreshWriteRate();
				}
				if (t - lastRefreshTime > refreshDelay)
					refresh();
			} else {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
					log.error(e);
				}
			}
		}

		// shut down.
		log.info("De-register nugget.");
		try {
			castleConnection.castle_deregister();
		} catch (Exception e) {
			log.warn("Error deregistering: " + e.getMessage());
		}
	}

	/**
	 * Read all information for the system into the 'dataMap' store of
	 * information. Scans the sysFs root for directories, and interprets each as
	 * a store of DA information for the DA with id equal to the directory name.
	 */
	public void refresh() {
		lastRefreshTime = System.currentTimeMillis();
		log.info("---- refresh ----");
		synchronized (syncLock) {
			// projections.clear();

			Set<Integer> toRemove = new HashSet<Integer>();
			toRemove.addAll(projections.keySet());

			File vertreesDir = new File(DAObject.sysFsRoot);
			File[] vertrees = vertreesDir.listFiles();
			for (int i = 0; i < vertrees.length; i++) {
				if (!vertrees[i].isDirectory())
					continue;
				Integer daId = fromHex(vertrees[i].getName());

				if (projections.containsKey(daId)) {
					try {
						projections.get(daId).refresh();
						toRemove.remove(daId);
					} catch (IOException e) {
						log.error("Error refreshing DA[" + hex(daId)
								+ "] -- removing");
					}
				} else {
					project(daId);
				}
			}

			// remove all the DAs that no longer appear or can't be read.
			for (Integer oldDaId : toRemove) {
				projections.remove(oldDaId);
			}
		}
	}

	@Override
	public SortedSet<Integer> daList() {
		synchronized (syncLock) {
			return projections.navigableKeySet();
		}
	}

	@Override
	public DAControlServer projectControl(int daId) {
		return project(daId);
	}

	@Override
	public DAView projectView(int daId) {
		return project(daId);
	}

	@Override
	public void setController(CastleController controller) {
		if (controller == null)
			return;
		log.info("set controller " + controller.toString());
		if (this.controller != null)
			throw new RuntimeException(
					"Cannot register multiple controllers times.");

		this.controller = controller;
		listeners.add(controller);
	}

	@Override
	public void addListener(CastleListener listener) {
		if (listener == null)
			return;
		log.info("add listener " + listener.toString());
		listeners.add(listener);
	}

	/**
	 * Return the server projection for the given DA. If this was previously
	 * unknown then fetch the info for it (by creating a new projection), and
	 * send an event to listeners.
	 * 
	 * @param daId
	 *            the DA to fetch a server for
	 * @return the server that deals with the given da.
	 */
	private DAControlServerImpl project(int daId) {
		if (projections.containsKey(daId)) {
			return projections.get(((Integer) daId));
		}
		synchronized (syncLock) {
			try {
				DAData data = new DAData(daId);
				DAControlServerImpl p = new DAControlServerImpl(data);
				projections.put(daId, p);

				// notify interested parties
				handleNewDA(data);

				return p;
			} catch (IOException e) {
				log.error("Unable to project to DA[" + hex(daId) + "]: "
						+ e.getMessage());
				return null;
			}
		}
	}

	/**
	 * Called by CastleEventThread when castle generates an event. In turn, the
	 * state of the cache is maintained, and the nugget is then informed.
	 * 
	 * TODO -- add new DA event TODO -- add DA destroyed event
	 */
	public void castleEvent(String s) {

		String[] args = new String[5];
		String[] prefixes = new String[] { "CMD=", "ARG1=0x", "ARG2=0x",
				"ARG3=0x", "ARG4=0x" };
		StringTokenizer tokenizer = new StringTokenizer(s, ":");
		int i;

		/* Return if there are no listeners. */
		if (listeners.isEmpty()) {
			log.info("castle event - No listeners registered.\n");
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
				if (args[0].equals("131")) {
					// parse "newArray" event.

					int arrayId = Integer.parseInt(args[2], 16);
					int daId = Integer.parseInt(args[3], 16);

					log.info("castle event - new array=A[" + hex(arrayId)
							+ "] da=" + hex(daId));

					DAControlServerImpl p = project(daId);
					if (p != null)
						p.handleNewArray(arrayId);

				} else if (args[0].equals("132")) {
					// parse "workDone" event.

					Integer workId = Integer.parseInt(args[2], 16);
					int workDone = Integer.parseInt(args[3], 16);
					int isMergeFinished = Integer.parseInt(args[4], 16);

					MergeWork work = mergeWorks.remove(workId);
					if (work == null) {
						log.error("Got event for non-started work W["
								+ hex(workId) + "], workDone=" + workDone
								+ ", finished=" + isMergeFinished);
					} else {
						DAControlServerImpl p = project(work.daId);
						if (p != null)
							p.handleWorkDone(work, workDone,
									isMergeFinished != 0);
					}
				} else {
					throw new RuntimeException("Unknown event");
				}
			}
		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}

	/**
	 * Handle the 'da created' castle event by passing the event along to all
	 * listeners.
	 */
	private void handleNewDA(DAInfo daInfo) {
		synchronized (syncLock) {
			for (CastleListener cl : listeners) {
				cl.newDA(daInfo);
			}
		}
	}

	/**
	 * Handle the 'da destroyed' castle event by removing a facet for the given
	 * da, and passing the event along to all listeners.
	 */
	private void handleDADestroyed(int _daId) {
		Integer daId = _daId;
		synchronized (syncLock) {
			projections.remove(daId);
			for (CastleListener cl : listeners) {
				cl.daDestroyed(daId);
			}
		}
	}

	/**
	 * Read multiple lines from a file.
	 */
	private static List<String> readLines(String directory, String filename)
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

			l.add(fromHex(h));
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

			l.add(fromHex(h));
		}
		return l;
	}

	/**
	 * Project to a particular DA.
	 * 
	 * @author andrewbyde
	 */
	class DAControlServerImpl implements DAControlServer {
		final DAData data;
		final int daId;

		// prefix for all logging messages, to identify the DA in question
		final String ids;

		private Double writeCeiling;

		// keep track of write rate
		private WorkProgressTracker writeProgress = new WorkProgressTracker(
				longTimeInterval);

		// keep track of merge rate
		private WorkProgressTracker mergeProgress = new WorkProgressTracker(
				longTimeInterval);

		// last time at which we observed the total bytes written
		private long lastWrittenTime = 0l;

		// last observation of total bytes written
		private Long lastWritten = null;

		private double recentWriteWork(long written) {
			double x = (lastWritten == null) ? 0.0 : (written - lastWritten)
					/ Utils.mbDouble;
			lastWritten = written;
			return x;
		}

		/**
		 * Constructor in which we read in to 'data' the information from sys/fs
		 * describing the arrays, value extents and merges in the given DA.
		 * 
		 * @param data
		 *            a container for information relating to a particular DA.
		 * @throws IOException
		 *             if sys/fs is unavailable
		 */
		DAControlServerImpl(DAData data) throws IOException {
			if (data == null)
				throw new IllegalArgumentException(
						"Cannot project onto null data");

			this.data = data;
			this.daId = data.daId;
			ids = "srv." + data.ids + " ";

			readData();
		}

		public String toString() {
			return "DAControlServer" + data.ids;
		}

		/**
		 * Re-read all data from scratch.
		 */
		void refresh() throws IOException {
			data.clear();
			readData();
			refreshWriteRate();
		}

		/**
		 * Called only by the constructor to a DA control server, to read a
		 * fresh copy of all data from /sys/fs.
		 */
		private void readData() throws IOException {
			String ids = this.ids + "readData ";
			log.debug(ids);
			synchronized (syncLock) {

				// parse array information. Need separate list to preserve order
				// info
				List<Integer> arrayIds = readQuantifiedIdList(data
						.sysFsString(), "array_list");
				int i = 0;
				for (Integer id : arrayIds) {
					data.putArray(id, i++, fetchArrayInfo(id));
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
					int id = fromHex(dirs[j].getName());
					data.putValueEx(id, fetchValueExInfo(id));
				}

				// parse merge information
				rootDir = new File(data.sysFsString(), "merges");
				dirs = rootDir.listFiles();
				for (int j = 0; j < dirs.length; j++) {
					if (!dirs[j].isDirectory())
						continue;
					int id = fromHex(dirs[j].getName());
					MergeInfo mInfo = fetchMergeInfo(id);
					data.putMerge(id, mInfo);

					syncVEMergeStates(mInfo);
				}

				if (log.isDebugEnabled()) {
					log.debug(ids + "arrays=" + hex(data.arrayIds));
					log.debug(ids + "value extents=" + hex(data.valueExIds));
					log.debug(ids + "merges=" + hex(data.mergeIds));
				}
			}
		}

		/**
		 * Update write ceiling and write rate.
		 */
		private void refreshWriteRate() {
			try {
				List<String> lines = null;
				lines = readLines(data.sysFsString(), "io_stats");
				if (lines == null) {
					log.error(ids + "io_stats file is null");
					return;
				}

				if (lines.size() < 7) {
					log.error(ids + "io_stats file has wrong length"
							+ " -- expected 7 lines, got " + lines.size());
					return;
				}

				// write ceiling
				String s = lines.get(0);
				writeCeiling = Long.parseLong(s.substring(12)) / Utils.mbDouble;

				// write rate, inline and outline
				s = lines.get(3);
				long inline = Long.parseLong(s.substring(17));
				s = lines.get(4);
				long outline = Long.parseLong(s.substring(18));

				long t = System.currentTimeMillis();
				double work = recentWriteWork(inline + outline);
				if ((lastWrittenTime != 0l) && (work > 0.0)) {
					synchronized (writeProgress) {
						writeProgress.add(lastWrittenTime, t, work);
					}
				}
				lastWrittenTime = t;

				if (log.isTraceEnabled())
					log.debug("write progress = " + writeProgress);
			} catch (Exception e) {
				log.error(ids + "Could not read sys fs entry for io_stats: "
						+ e.getMessage());
			}
		}

		/** Write max, that castle is considering as a threshold. */
		@Override
		public Double getWriteCeiling() {
			return writeCeiling;
		}

		/** Observed write rate, MB/s. */
		@Override
		public Double getWriteRate() {
			synchronized (writeProgress) {
				return writeProgress.rate(shortTimeInterval);
			}
		}

		@Override
		public Double getWriteRateLong() {
			synchronized (writeProgress) {
				return writeProgress.rate();
			}
		}

		@Override
		public Double getMergeRate() {
			synchronized (mergeProgress) {
				return mergeProgress.rate(shortTimeInterval);
			}
		}

		@Override
		public Double getMergeRateLong() {
			synchronized (mergeProgress) {
				return mergeProgress.rate();
			}
		}

		@Override
		public DAInfo getDAInfo() {
			return data;
		}

		@Override
		public ArrayInfo getArrayInfo(int id) {
			synchronized (syncLock) {
				ArrayInfo info = data.getArray(id);
				if (info == null)
					return null;

				syncArraySizes(info);
				return info;
			}
		}

		@Override
		public MergeInfo getMergeInfo(int id) {
			synchronized (syncLock) {
				MergeInfo info = data.getMerge(id);
				if (info == null)
					return null;

				syncMergeSizes(info);
				return info;
			}
		}

		@Override
		public ValueExInfo getValueExInfo(int id) {
			synchronized (syncLock) {
				ValueExInfo info = data.getValueEx(id);
				if (info == null)
					return null;

				// refresh sizes
				try {
					syncValueExSizes(info);
				} catch (IOException e) {
					// something wrong with this VE info. Purge
					log.error("Could not sync value extent sizes for "
							+ info.ids + ": " + e.getMessage() + ", removing.");
					data.removeValueEx(id);
				}
				return info;
			}
		}

		/**
		 * Keep merge size synced
		 */
		private void syncMergeSizes(MergeInfo info) {
			if (info == null)
				return;

			try {
				String l = readLine(info.sysFsFile, "progress");
				StringTokenizer st = new StringTokenizer(l);
				if (st.hasMoreTokens()) {
					info.workDone = Long.parseLong(st.nextToken());
				}
				if (st.hasMoreTokens()) {
					info.workTotal = Long.parseLong(st.nextToken());
				}
			} catch (Exception e) {
				log.error("Could not sync merge sizes for merge " + info.ids
						+ ": " + e.getMessage());
				if (failOnError) {
					System.exit(-1);
				} else {
					killMerge(info);
				}
			}
		}

		/**
		 * Kills an array -- just removes if from the DAData.
		 * 
		 * @param id
		 *            the id of array to kill
		 */
		private void killArray(Integer id) {
			data.removeArray(id);
		}

		/**
		 * Remove merge info from DAData; also kill input arrays and the
		 * 'extentsToDrain' (TODO -- might be incorrect in future if multiple
		 * arrays point to the same VE); set output arrays to 'NOT_MERGING'. If
		 * the mergeInfo does not exist in the data then this does nothing.
		 * 
		 * @param mergeInfo
		 *            the merge to kill
		 */
		private void killMerge(MergeInfo mergeInfo) {
			if (mergeInfo == null)
				return;

			if (data.containsMerge(mergeInfo.id)) {
				log.warn("Attempt to kill a non-existent merge "
						+ mergeInfo.ids);
			}

			log.info("kill merge " + mergeInfo.id);
			/*
			 * remove input value extents TODO -- is this always correct? might
			 * be shared VEs in general
			 */
			if (mergeInfo.extentsToDrain == null) {
				mergeInfo.extentsToDrain = getAllVEs(mergeInfo.inputArrayIds);
				log.info(ids + "null extents to drain, so set to ALL = "
						+ mergeInfo.extentsToDrain);
			}

			// remove arrays
			for (Integer id : mergeInfo.inputArrayIds) {
				killArray(id);
			}

			for (Integer id : mergeInfo.extentsToDrain) {
				data.removeValueEx(id);
			}

			// output VE, if it exists, is no longer merging
			ValueExInfo vInfo = data.getValueEx(mergeInfo.outputValueExtentId);
			if (vInfo != null)
				vInfo.mergeState = MergeState.NOT_MERGING;

			// sync state of output arrays
			for (Integer id : mergeInfo.outputArrayIds) {
				ArrayInfo info = getArrayInfo(id);
				info.mergeState = ArrayInfo.MergeState.NOT_MERGING;
			}

			// remove merge
			Integer mId = mergeInfo.id;
			data.removeMerge(mId);
			log.info(ids + "removed merge " + mergeInfo.ids + ", now merges = "
					+ hex(data.mergeIds));
		}

		/**
		 * Read and keep up-to-date the size for the given array. Updates the
		 * size variables.
		 * 
		 * @param info
		 *            the array info to sync
		 */
		private void syncArraySizes(ArrayInfo info) {
			if (info == null)
				return;

			try {
				// read the data...
				List<String> lines = readLines(info.sysFsFile, "size");
				if (lines.size() < 4)
					throw new RuntimeException("got " + lines.size()
							+ " < 4 lines in 'size' file");

				// 'Item Count: <count>'
				info.itemCount = Long.parseLong(lines.get(0).substring(12));

				// 'Reserved Bytes: <bytes>'
				info.reservedSizeInBytes = Long.parseLong(lines.get(1)
						.substring(16));

				// 'Used Bytes: <bytes>'
				info.usedInBytes = Long.parseLong(lines.get(2).substring(12));

				// 'Current Size Bytes: <bytes>'
				info.currentSizeInBytes = Long.parseLong(lines.get(3)
						.substring(20));
			} catch (Exception e) {
				log.error("Could not sync array sizes for " + info.ids + ": "
						+ e.getMessage());
				if (failOnError)
					System.exit(-1);
				else {
					killArray(info.id);
				}
			}
		}

		/**
		 * Read and keep up-to-date the size for the given array.
		 * 
		 * @param info
		 *            the value extent info. Updates the size in bytes and
		 *            number of entries.
		 */
		private void syncValueExSizes(ValueExInfo info) throws IOException {
			if (info == null)
				return;

			// assemble size variables
			List<String> lines = readLines(info.sysFsFile, "size");

			// 'Bytes: <bytes>'
			info.sizeInBytes = Long.parseLong(lines.get(0).substring(7));

			// 'Entries: <entries>'
			info.numEntries = Long.parseLong(lines.get(1).substring(9));
		}

		/**
		 * Fetch info for a new array and ensure all data structures are
		 * up-to-date.
		 * 
		 * @param location
		 *            where in the list of arrays to register this new one.
		 * @param id
		 *            the id of the array.
		 * @return the info fetched.
		 */
		private ArrayInfo newArray(int location, int id) {
			synchronized (syncLock) {
				if (log.isDebugEnabled())
					log.debug("new array A[" + hex(id) + "] at location "
							+ location);
				ArrayInfo info = null;
				try {
					info = fetchArrayInfo(id);
				} catch (IOException e) {
					log.error(ids + "newArray - unable to fetch info for A["
							+ hex(id) + "]");
				}
				if (info == null)
					return null;

				// put the new array at the head of the list.
				data.putArray(id, location, info);
				if (log.isDebugEnabled())
					log.info("now arrays are " + hex(data.arrayIds));

				// ensure that all these value extents are known about.
				if (info.valueExIds != null) {
					for (Integer vId : info.valueExIds) {
						newVE(vId);
					}
				}
				return info;
			}
		}

		/**
		 * Fetch and load into 'data' a new value extent info with the given id.
		 * If the ValueExtent already exists in the data, then simply return it.
		 * 
		 * @param id
		 *            the id of the value info to fetch and add.
		 * @return the ValueExInfo fetched.
		 */
		private ValueExInfo newVE(Integer id) {
			if (id == null)
				return null;
			if (log.isDebugEnabled()) {
				log.debug("new VE[" + hex(id) + "]");
			}
			synchronized (syncLock) {
				// if (data.containsVE(id))
				// return data.getValueEx(id);
				if (data.containsVE(id))
					return data.getValueEx(id);
				ValueExInfo info = null;
				try {
					info = fetchValueExInfo(id);
				} catch (IOException e) {
					log.error(ids + "newVE - unable to fetch info for VE["
							+ hex(id) + "]");
				}
				if (info == null)
					return null;
				data.putValueEx(id, info);
				return info;
			}
		}

		/**
		 * Since merge state is not persisted for value extents, derive it from
		 * the fact that they are involved in a merge.
		 */
		private void syncVEMergeStates(MergeInfo mInfo) {
			// fix merge state of VEs drained by the merge
			SortedSet<Integer> vIds = mInfo.extentsToDrain;
			// null means all...
			if (vIds == null) {
				vIds = getAllVEs(mInfo.inputArrayIds);
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
		private SortedSet<Integer> getAllVEs(List<Integer> aIds) {
			if (aIds == null)
				return null;
			SortedSet<Integer> vIds = new TreeSet<Integer>();
			for (Integer aId : aIds) {
				ArrayInfo aInfo = data.getArray(aId);
				vIds.addAll(aInfo.valueExIds);
			}
			return vIds;
		}

		/**
		 * Read the information for a single array. At present it does not
		 * log.info merging information correctly. FileNotFound -> array has
		 * just been deleted, so ignore and return null.
		 */
		private ArrayInfo fetchArrayInfo(int id) throws IOException {
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
		private MergeInfo fetchMergeInfo(int id) throws IOException {
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
		private ValueExInfo fetchValueExInfo(int id) throws IOException {
			if (data == null)
				return null;
			synchronized (syncLock) {
				ValueExInfo info = new ValueExInfo(data.daId, id);

				if (!info.sysFsFile.exists())
					return null;

				// assemble size variables
				syncValueExSizes(info);

				return info;
			}
		}

		// ///////////////////////////////////////////////////////////////////////
		// Control interface
		// ///////////////////////////////////////////////////////////////////////

		/** Set an upper bound on the write rate, in MB/s. */
		@Override
		public void setWriteRate(double rateMB) {
			try {
				int r = (int) rateMB;
				if (rateMB > Integer.MAX_VALUE)
					r = Integer.MAX_VALUE;

				writeCeiling = (double) r;
				log.debug(ids + "set write rate to " + r);
				castleConnection.insert_rate_set(daId, (int) rateMB);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		/** Set target read bandwidth, MB/s. */
		@Override
		public void setReadRate(double rateMB) {
			try {
				int r = (int) rateMB;
				if (rateMB > Integer.MAX_VALUE)
					r = Integer.MAX_VALUE;

				log.debug(ids + "set read rate to " + r);
				castleConnection.read_rate_set(daId, r);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		/**
		 * Start a merge using the given merge config. Updates array info state
		 * to the correct merge state.
		 */
		@Override
		public MergeInfo startMerge(MergeConfig mergeConfig) throws CastleException {
			try {
				synchronized (syncLock) {
					log.info(ids + "start merge " + mergeConfig.toStringLine());

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
							log.error(ids + "start merge cannot use array A["
									+ arrayId + "]");
							throw new IllegalArgumentException(
									"Cannot start a merge on non-existant array A["
											+ hex(arrayId) + "]");
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
					int location = data
							.maxIndexOfArray(mergeConfig.inputArrayIds) + 1;

					log.debug(ids + "call merge_start");
					/* WARNING: this hardcodes c_rda_type_t. */
					int mergeId = castleConnection.merge_start(arrayIds,
							dataExtentsToDrain, 0, 0, 0);
					log.debug(ids + "merge_start returned merge id "
							+ hex(mergeId));

					// this will do the fetch and cache update for the merge.
					MergeInfo mergeInfo = fetchMergeInfo(mergeId);
					if (mergeInfo == null) {
						throw new CastleException(0, "Null merge info for merge "
								+ mergeId);
					} else {
						log.info(ids + "new merge=" + mergeInfo.toStringLine());
					}
					data.putMerge(mergeId, mergeInfo);

					// update input arrays
					for (Integer id : mergeInfo.inputArrayIds) {
						// no size sync needed
						ArrayInfo info = data.getArray(id);
						info.mergeState = ArrayInfo.MergeState.INPUT;
					}

					// output arrays are new, so fetch and add to DAData
					for (Integer id : mergeInfo.outputArrayIds) {
						ArrayInfo info = newArray(location, id);
						info.mergeState = ArrayInfo.MergeState.OUTPUT;
					}

					// output value extent (if any) is new,
					// so fetch and add to DAData
					Integer vId = mergeInfo.outputValueExtentId;
					if (vId != null) {
						ValueExInfo vInfo = fetchValueExInfo(vId);
						log.debug(ids + "get info for output " + vInfo.ids);
						data.putValueEx(vId, vInfo);
					}
					syncVEMergeStates(mergeInfo);

					return mergeInfo;
				}
			} catch (Exception e) {
				log.error(ids + "merge start failed: " + e.getMessage());
				log.error(e);
				e.printStackTrace();
				return null;
			}
		}

		@Override
		public int doWork(int mergeId, long mergeUnits) throws CastleException {
			synchronized (syncLock) {
				log.info(ids + "do work on M[" + hex(mergeId) + "]"
						+ ", units=" + mergeUnits);

				// fail early if there's no such merge.
				MergeInfo mergeInfo = getMergeInfo(mergeId);
				if (mergeInfo == null)
					throw new CastleException(CastleException.Error.ABSENT, "merge " + hex(mergeId));

				// submit the work
				int workId = castleConnection
						.merge_do_work(mergeId, mergeUnits);

				// construct the work
				MergeWork work = new MergeWork(mergeInfo, workId, mergeUnits);
				mergeWorks.put(workId, work);
				log.debug(ids + " work unit " + work.ids + " for merge "
						+ mergeInfo.ids);

				return workId;
			}
		}

		/**
		 * Deal with an event from castle: new array. Ignores arrays we already
		 * know about.
		 * 
		 * @param arrayId
		 *            id of the new array.
		 */
		void handleNewArray(int arrayId) {
			synchronized (syncLock) {
				// if this is the new output of a merge, then we already
				// know about it.
				if (data.containsArray(arrayId)) {
					log.debug(ids + "handle new array A[" + hex(arrayId)
							+ "] -- already known");
				} else {
					// when starting merges we add arrays, so this
					// code should only be reached for T0s.

					ArrayInfo info = newArray(0, arrayId);
					log.info(ids + "handle new array " + info.ids
							+ ", send event to listeners");
					if (info != null) {
						for (CastleListener cl : listeners) {
							cl.newArray(info);
						}
					}
				}
			}
		}

		/**
		 * Handle a Castle event corresponding to the end of a piece of merge
		 * work. Keep internal state correct, and propagate workDone event to
		 * nugget.
		 */
		void handleWorkDone(MergeWork work, long workDone,
				boolean isMergeFinished) {
			String ids = this.ids + "handleWorkDone ";

			// TODO workDone cannot be trusted
			double w = work.mergeUnits / Utils.mbDouble;

			// long t = System.currentTimeMillis();
			synchronized (mergeProgress) {
				// mergeProgress.add(work.startTime, t, w);
				mergeProgress.add(w);
				if (log.isDebugEnabled())
					log.debug("merge progress = " + writeProgress);
			}

			MergeInfo mergeInfo = work.mergeInfo;
			if (mergeInfo == null) {
				log.error(ids + "cannot handle work for null merge");
				return;
			}

			// update array sizes and merge state
			try {
				synchronized (syncLock) {
					log.info(ids + work.ids + ", workDone=" + workDone + ", "
							+ mergeInfo.ids
							+ (isMergeFinished ? " FINISHED" : ""));
					/*
					 * if finished, remove the merge and input arrays from the
					 * cache pre-emptively
					 */
					if (isMergeFinished) {
						killMerge(mergeInfo);
					} else {
						// sync work done.
						syncMergeSizes(mergeInfo);

						// update array sizes
						for (Integer id : mergeInfo.inputArrayIds) {
							ArrayInfo info = getArrayInfo(id);
							syncArraySizes(info);
						}
						for (Integer id : mergeInfo.outputArrayIds) {
							ArrayInfo info = getArrayInfo(id);
							syncArraySizes(info);
						}

					}
				}
			} catch (Exception e) {
				log.error(ids + "Could not sync after work finished: "
						+ e.getMessage());
				return;
			}
			log.debug(ids + "data synced.  Inform listeners");
			for (CastleListener cl : listeners) {
				try {
					cl.workDone(data.daId, work.workId,
							(workDone != 0) ? work.mergeUnits : 0,
							isMergeFinished);
				} catch (Exception e) {
					log.error(ids + "error while informing listener: "
							+ e.getMessage());
				}
			}
		}

	}

}

/**
 * Castle data holds more than castle info, because DAData holds more than
 * DAInfo.
 */
class CastleData {
	final private HashMap<Integer, DAData> dataMap = new HashMap<Integer, DAData>();

	public void clear() {
		dataMap.clear();
	}

	public void put(int daId, DAData data) {
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
	private static Logger log = Logger.getLogger(DAData.class);

	private HashMap<Integer, ArrayInfo> arrays = new HashMap<Integer, ArrayInfo>();
	private HashMap<Integer, MergeInfo> merges = new HashMap<Integer, MergeInfo>();
	private HashMap<Integer, ValueExInfo> values = new HashMap<Integer, ValueExInfo>();

	DAData(int daId) {
		super(daId, new ArrayList<Integer>(), new TreeSet<Integer>(),
				new TreeSet<Integer>());
	}

	void clear() {
		log.debug("clear");
		synchronized (CastleControlServerImpl.syncLock) {
			arrayIds.clear();
			arrays.clear();
			mergeIds.clear();
			merges.clear();
			valueExIds.clear();
			values.clear();
		}
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

	boolean containsMerge(Integer id) {
		return merges.containsKey(id);
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

	public String toStringOneLine() {
		StringBuilder sb = new StringBuilder();
		List<Integer> aids = new LinkedList<Integer>();
		synchronized (CastleControlServerImpl.syncLock) {
			aids.addAll(arrayIds);
		}
		sb.append("A: ");
		for (Iterator<Integer> it = aids.iterator(); it.hasNext(); ) {
			Integer aid = it.next();
			
			ArrayInfo info = arrays.get(aid);
			if (info == null)
				continue;
			if (info.mergeState == MergeState.OUTPUT)
				sb.append("+");
			sb.append(hex(info.id));
			if (info.mergeState == MergeState.INPUT)
				sb.append("-");
			
			if (it.hasNext()) {
				sb.append(", ");
			}
		}

		List<Integer> mids = new LinkedList<Integer>();
		synchronized (CastleControlServerImpl.syncLock) {
			mids.addAll(mergeIds);
		}
		sb.append(", M: ");
		for(Iterator<Integer> it = mids.iterator(); it.hasNext(); ) {
			Integer mid = it.next();
			MergeInfo info = merges.get(mid);
			if (info == null)
				continue;
			List<Integer> in = info.inputArrayIds;
			List<Integer> out = info.outputArrayIds;
			SortedSet<Integer> drain = info.extentsToDrain;
			
			sb.append(hex(info.id) + "{" + hex(in) + "->" + hex(out) + " drain" + hex(drain) + "}");
			if (it.hasNext())
				sb.append(", ");
		}
		
		sb.append(", VE: " + hex(valueExIds));
		return sb.toString();
	}
}
