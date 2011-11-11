package com.acunu.castle.control;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.log4j.Logger;

import com.acunu.castle.Castle;
import com.acunu.castle.CastleError;
import com.acunu.castle.CastleException;
import com.acunu.castle.EventListener;
import com.acunu.castle.control.ArrayInfo.MergeState;
import com.acunu.util.DeadManSwitch;
import com.acunu.util.Properties;
import com.acunu.util.Utils;
import com.acunu.util.WorkProgressTracker;

import static com.acunu.castle.control.HexWriter.*;

/**
 * An implementation of {@linkplain CastleControlServer}. A proxy to direct
 * events from castle to a nugget, and vice-versa from a nugget down to castle.
 */
public class CastleControlServerImpl implements
		CastleControlServer, Runnable {
	private static final Logger log = Logger.getLogger(CastleControlServerImpl.class);
	private static final boolean isTrace = log.isTraceEnabled();
	private static final boolean isDebug = log.isDebugEnabled();
	
	protected static final String ids = "srv ";

	/** delay (ms) between calls to {@linkplain #refresh}. */
	private static final long refreshDelay = 60000;
	
	/** for rate measurements. */
	private static final long shortTimeInterval = 1000l;

	/** for rate measurements. */
	private static final long longTimeInterval = 10000l;

	/** refresh the write rate estimates three times per second. */
	private static final long writeRateDelay = 300;

	/** delay between heartbeats to Castle. */
	private static final long heartbeatDelay = 1000l;

	private final Set<CastleListener> listeners = Collections.synchronizedSet(new HashSet<CastleListener>());

	/** Projections of this castle server onto each DA. */
	private final NavigableMap<Integer, DAControlServerImpl> projections = new ConcurrentSkipListMap<Integer, DAControlServerImpl>();

	/** List of on-going work, indexed by work id. */
	private final Map<Integer, MergeWork> mergeWorks = new ConcurrentSkipListMap<Integer, MergeWork>();

	/**
	 * Server. Used by the control actions 'startMerge' and 'doWork'; generates
	 * events that call the 'castleEvent' method on this class.
	 */
	private final Castle castleConnection;

	private final DeadManSwitch deadManSwitch;

	private final Thread runThread;
	
	/**
	 * TODO -- as a temporary fix for events being delivered late (e.g. 45
	 * seconds late!) optionally watch the contents of each DA's array list to
	 * see if there are entries we don't recognized.
	 */
	private final boolean watch;

	private final double pMonkeyHeartbeat;
	
	/**
	 * Client. Pass server events to the nugget after synchronizing state
	 * information, and likewise pass control messages down then synchronize
	 * state information.
	 */
	private CastleController controller = null;

	/** used to sync at least once in a while. */
	private long lastRefreshTime = 0;

	private int pid = 0;

	/**
	 * Boolean to control the runThread. When set to false the thread will
	 * exit.
	 */
	private boolean running = true;

	private int exitCode = 0;

	/**
	 * Constructor, in which we attempt to bind to the server. Two threads are
	 * spawned -- one 'castle_evt' which handles castle events using a
	 * CastleEventsThread object; the other 'ctrl_sync' which regularly
	 * refreshes the CNS's view of the server by calling the refresh method.
	 * 
	 * @see #refresh()
	 * @throws IOException
	 *             if a connection to castle cannot be established -- for
	 *             example if another nugget is already connected.
	 */
	public CastleControlServerImpl(Properties props, DeadManSwitch ds) throws IOException {

		pid = Utils.pid();
		log.info(ids + "---- create ---- pid = " + pid);

		watch = props.getBoolean("gn.watch", true);

		log.debug(ids + "connect...");
		castleConnection = new Castle(new HashMap<Integer, Integer>(), false);
		log.debug(ids + "connected.");

		pMonkeyHeartbeat = props.getDouble("gn.pMonkeyHeartbeat", 0.0);

		deadManSwitch = ds;
		runThread = new Thread(this);
		runThread.setName("srv_" + pid);
		// now we start monitoring things
		runThread.start();

		// finally, make events happen
		castleConnection.startEventListener(new EventListener() {
			@Override
			public void udevEvent(String s) {
				castleEvent(s);
			}

			@Override
			public void error(Throwable t) {
				log.error("Error in events thread", t);
				running = false;
			}
		});
	}

	@Override
	public int join() throws InterruptedException {
		runThread.join();
		return exitCode;
	}

	/**
	 * Run. Regularly refreshes the write rate, and periodically re-reads all
	 * data.
	 */
	public void run() {
		// register.
		log.info(ids + "Register nugget");
		try {
			castleConnection.castle_register();
			log.info(ids + " registered");
		} catch (CastleException e) {
			exitCode = 1;
			log.error("Error registering: " + e, e);
			return;
		}

		Random r = new Random();

		long lastHeartbeatTime = 0l;
		long lastWriteTime = 0l;

		try {
			while (running) {
				long t = System.currentTimeMillis();

				if (t - lastHeartbeatTime > heartbeatDelay) {
					lastHeartbeatTime = t;
					try {
						log.debug("call castle_heartbeat");
						castleConnection.castle_heartbeat();
						deadManSwitch.heartbeat();
					} catch (CastleException e) {
						log.error("Heartbeat failed (exit): " + e, e);
						exitCode = 1;
						return;
					}

					// maybe watch the array lists for each DA
					if (watch) {
							for (DAControlServerImpl s : projections.values()) {
								s.watchArrays();
						}
					}

					// occasionally wait 15 s so as to deliberately lose
					// contact with castle.
					if (r.nextDouble() < pMonkeyHeartbeat) {
						Utils.waitABit(12000);
					}
				}

				if (t - lastWriteTime > writeRateDelay) {
					// refresh all the write measurements
					lastWriteTime = t;
					for (DAControlServerImpl s : projections.values()) {
						s.refreshRates();
					}
				}

				// attempt to refresh
				if (t - lastRefreshTime > refreshDelay) {
					refresh();
				}

				// be nice...
				Thread.yield();
				Utils.waitABit(50);
			}
		} catch (Exception e) {
			log.error("Error while running (exit): " + e, e);
			exitCode = 1;
		} finally {
			// shut down.
			log.info("De-register nugget.");
			try {
				castleConnection.castle_deregister();
				log.info("De-registered");
			} catch (CastleException e) {
				log.error("Error deregistering: " + e, e);
				exitCode = 1;
			}
			try {
				castleConnection.disconnect();
			} catch (IOException e) {
				log.error("Error disconnecting from Castle", e);
				exitCode = 1;
			}
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
		synchronized (projections) {
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

			// report on-going work
		if (log.isInfoEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append(ids + "on-going work: ");
			long y = System.currentTimeMillis();
			for (MergeWork mw : mergeWorks.values()) {
				long dt = y - mw.startTime;
				double duration = dt / 1000.0;
				String t = (duration < 0.1) ? dt + "ms" : Utils.onePlace
						.format(duration);
				if (duration > 5.0) {
					t = "!" + t + "!";
				}

				sb.append(hex(mw.workId) + "(" + t + ")  ");
			}
			log.info(sb.toString());
		}
	}

	@Override
	public SortedSet<Integer> daList() {
		return new TreeSet<Integer>(projections.navigableKeySet());
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
		synchronized (projections) {
			if (projections.containsKey(daId)) {
				return projections.get(((Integer) daId));
			}

			try {
				DAData data = new DAData(daId);
				DAControlServerImpl p = new DAControlServerImpl(data);
				projections.put(daId, p);

				// notify interested parties
				handleNewDA(data);

				return p;
			} catch (IOException e) {
				log.error("Unable to project to DA[" + hex(daId) + "]: " + e, e);
				return null;
			}
		}
	}

	/**
	 * Called by CastleEventThread when castle generates an event. In turn, the
	 * state of the cache is maintained, and the nugget is then informed.
	 */
	public void castleEvent(String s) {

		String[] args = new String[6];
		String[] prefixes = new String[] { "NOTIFY=", "CMD=", "ARG1=0x",
				"ARG2=0x", "ARG3=0x", "ARG4=0x" };
		StringTokenizer tokenizer = new StringTokenizer(s, ":");
		int i;

		/* Return if there are no listeners. */
		if (listeners.isEmpty()) {
			log.info("castle event - No listeners registered.\n");
			return;
		}

		i = 0;
		while (tokenizer.hasMoreTokens() && (i < 6)) {
			args[i] = tokenizer.nextToken();
			if (!args[i].startsWith(prefixes[i]))
				throw new RuntimeException("Bad event string formatting: " + s);
			args[i] = args[i].substring(prefixes[i].length());
			i++;
		}

		if (i != 6)
			throw new RuntimeException("Bad event string formatting: " + s);

		try {
				if (args[1].equals("131")) {
					// parse "newArray" event.

					long arrayId = Long.parseLong(args[3], 16);
					int daId = Integer.parseInt(args[4], 16);

					log.info("castle event - new array=A[" + hex(arrayId)
							+ "] da=" + hex(daId));

					DAControlServerImpl p = project(daId);
					if (p != null)
						p.handleNewArray(arrayId);

				} else if (args[1].equals("132")) {
					// parse "workDone" event.

					Integer workId = Integer.parseInt(args[3], 16);
					int workDone = Integer.parseInt(args[4], 16);
					int isMergeFinished = Integer.parseInt(args[5], 16);

					MergeWork work = mergeWorks.remove(workId);
					if (work == null) {
						log.error("Got event for non-started work W["
								+ hex(workId) + "], workDone=" + workDone
								+ ", finished=" + isMergeFinished);

						/*
						 * How did we get here? The most likely explanation is
						 * that the previous nugget was killed, this one took
						 * over, and now we're hearing about merges that were
						 * on-going when the switch happened.
						 * 
						 * If so, then we need to see if there is a merge
						 * corresponding to this work ... normally this would be
						 * impossible, because we only have work id, not merge
						 * id. However ... TODO HACK ... in the current
						 * implementation, merge id and work id are the same.
						 * Therefore we can lookup merge by work id. Except that
						 * to do that we also need to know daId -- and that's
						 * not implemented for these messages.
						 */
					} else {
						DAControlServerImpl p = project(work.daId);
						if (p != null) {
							long t = System.currentTimeMillis();
							work.setWorkDone(workDone, t);
							p.handleWorkDone(work, isMergeFinished != 0);
						}
					}
				} else if (args[1].equals("133")) {
					// parse "new DA" event
					Integer daId = fromHex(args[3]);
					log.info("castle event - new da=" + hex(daId));
					project(daId);
				} else if (args[1].equals("134")) {
					// parse "DA destroyed" event
					Integer daId = fromHex(args[3]);
					log.info("castle event - da=" + hex(daId) + " destroyed");
					handleDADestroyed(daId);
				} else {
					throw new RuntimeException("Unknown event: '" + s + "'");
				}
		} catch (Exception e) {
			log.error("Error handling castle event: " + e, e);
		}
	}

	/**
	 * Handle the 'da created' castle event by passing the event along to all
	 * listeners.
	 */
	private void handleNewDA(DAInfo daInfo) {
		List<CastleListener> curListeners;
		synchronized (listeners) {
			curListeners = new ArrayList<CastleListener>(listeners);
		}
		for (CastleListener cl : curListeners) {
			cl.newDA(daInfo);
		}
	}

	/**
	 * Handle the 'da destroyed' castle event by removing a facet for the given
	 * da, and passing the event along to all listeners.
	 */
	private void handleDADestroyed(int daId) {
		synchronized (projections) {
			projections.remove(daId);
		}
		List<CastleListener> curListeners;
		synchronized (listeners) {
			curListeners = new ArrayList<CastleListener>(listeners);
		}
		for (CastleListener cl : curListeners) {
			cl.daDestroyed(daId);
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
	 * Parse a list of ids of the form '0x12 0x123' etc.
	 */
	private static List<Long> readIdList(File directory, String filename)
			throws IOException {
		String l = readLine(directory, filename);
		return idList(l);
	}

	/**
	 * Parse a list of ids of the form '0x12 0x123' etc.
	 */
	private static SortedSet<Long> readIdSet(File directory, String filename)
			throws IOException {
		String l = readLine(directory, filename);
		return idSet(l);
	}

	/**
	 * Within a directory, look for all sub-directories, and return their names
	 * as integers.
	 */
	private static SortedSet<Long> readIdFromSubdirs(File directory)
			throws IOException {
		SortedSet<Long> s = new TreeSet<Long>();
		if (directory == null)
			throw new IllegalArgumentException("Null directory");
		if (!directory.exists())
			throw new IllegalArgumentException("Directory '" + directory
					+ "' does not exist");
		if (!directory.isDirectory())
			throw new IllegalArgumentException("'" + directory
					+ "' is not a directory");

		File[] subDirs = directory.listFiles();
		for (int i = 0; i < subDirs.length; i++) {
			if (!subDirs[i].isDirectory())
				continue;
			s.add(fromHexL(subDirs[i].getName()));
		}
		return s;
	}

	private static Integer readHexInt(File directory, String filename)
			throws IOException {
		String l = readLine(directory, filename);
		StringTokenizer st = new StringTokenizer(l);
		String t = st.nextToken();
		return fromHex(t);
	}

	private static Long readHexLong(File directory, String filename)
			throws IOException {
		String l = readLine(directory, filename);
		StringTokenizer st = new StringTokenizer(l);
		String t = st.nextToken();
		return fromHexL(t);
	}

	/**
	 * Parse a list of ids of the form '0x12 0x123' etc.
	 */
	private static List<Long> idList(String idListString) {
		List<Long> l = new LinkedList<Long>();
		if (idListString == null)
			return l;
		StringTokenizer st = new StringTokenizer(idListString);
		while (st.hasMoreTokens()) {
			String h = st.nextToken();
			l.add(fromHexL(h));
		}
		return l;
	}

	/**
	 * Parse a list of ids of the form '0x12 0x123' etc.
	 * 
	 * @return a set of ids. Empty set if idListString is null. If we reach the
	 *         end of PAGE_SIZE then terminate and return whichever ones we've
	 *         found so far.
	 */
	private static SortedSet<Long> idSet(String idListString) {
		SortedSet<Long> l = new TreeSet<Long>();
		if (idListString == null)
			return null;
		StringTokenizer st = new StringTokenizer(idListString);
		while (st.hasMoreTokens()) {
			String h = st.nextToken();
			if (h.startsWith("Overflow"))
				return l;
			l.add(fromHexL(h));
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

		/** Ceiling on the write rate. */
		private Double writeCeiling;

		/**
		 * Ceiling on the read rate. Do not remove -- will be used in future
		 * incarnations.
		 */
		private Double readCeiling;

		/** keep track of write rate. */
		private WorkProgressTracker writeProgress = new WorkProgressTracker(
				longTimeInterval);

		/** keep track of read rate. */
		private WorkProgressTracker readProgress = new WorkProgressTracker(
				longTimeInterval);

		/** keep track of merge rate. */
		private WorkProgressTracker mergeProgress = new WorkProgressTracker(
				longTimeInterval);

		private final File sysFsArrays;

		/** last time at which we observed the total bytes written. */
		private long lastWrittenTime = 0l;

		/** last observation of total bytes written. */
		private Long lastWritten = null;

		private long lastReadTime = 0l;
		private Long lastRead = null;

		private double totalWritten;
		private double totalRead;

		/**
		 * Keep track of the total written, and return from here the amount
		 * written since the last time this method was called.
		 */
		private void recentWriteWork(long read, long written) {
			long t = System.currentTimeMillis();			
			double x = (lastWritten == null) ? 0.0 : (written - lastWritten)
					/ Utils.mbDouble;
			lastWritten = written;

			if ((lastWrittenTime != 0l) && (x > 0.0)) {
				synchronized (writeProgress) {
					writeProgress.add(lastWrittenTime, t, x);
				}
			}
			lastWrittenTime = t;

			x = (lastRead == null) ? 0.0 : (read - lastRead) / Utils.mbDouble;
			lastRead = read;

			if ((lastReadTime != 0l) && (x > 0.0)) {
				synchronized (readProgress) {
					readProgress.add(lastReadTime, t, x);
				}
			}
			lastReadTime = t;
		}

		/**
		 * Constructor in which we read in to 'data' the information from sys/fs
		 * describing the arrays, value extents and merges in the given DA.
		 * 
		 * @param data
		 *            a container for information relating to a particular DA.
		 * @throws IOException
		 *             if sys/fs is unavailable or unreadable
		 */
		DAControlServerImpl(DAData data) throws IOException {
			if (data == null)
				throw new IllegalArgumentException(
						"Cannot project onto null data");

			this.data = data;
			this.daId = data.daId;
			ids = "srv." + data.ids + " ";
			sysFsArrays = new File(data.sysFsString(), "arrays");

			// read the data for this da. throws an io exception if we can't
			// read sys fs
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
			refreshRates();
		}

		/**
		 * Look at the arrays directory; if there's one we don't know about,
		 * then let it be known!
		 */
		private void watchArrays() {
			File[] arrays = sysFsArrays.listFiles();
			for (int j = 0; j < arrays.length; j++) {
				if (!arrays[j].isDirectory())
					continue;
				Long id = fromHexL(arrays[j].getName());
				if (!data.containsArray(id))
					handleNewArray(id);
				else {
					// sync merge state -- many new arrays have their state set
					// as output for no good reason!
					ArrayInfo info = data.getArray(id);
					if (info == null)
						continue;
					try {
						MergeState prev = info.mergeState;
						String s = readLine(info.sysFsFile, "merge_state");
						info.setMergeState(s);
						MergeState newS = info.mergeState;
						if (newS != prev) {
							log.warn(ids
									+ " while watching arrays, changed merge state of "
									+ info.ids + " from " + prev + " to "
									+ newS);
						}
					} catch (IOException e) {
						// error almost surely means the array has been removed.
						log.warn(ids + " could not sync merge state for "
								+ info.ids + ": " + e.getMessage(), e);
					}
				}
			}
		}

		/**
		 * Called only by the constructor to a DA control server, to read a
		 * fresh copy of all data from sys fs.
		 * 
		 * @throws IOException
		 *             if there is an IO exception reading from sys fs for any
		 *             object.
		 */
		private void readData() throws IOException {
			String ids = this.ids + "readData ";
			log.debug(ids);
			try {
				File rootDir = null;
				File[] dirs = null;

				// arrays
				SortedSet<ArrayInfo> arr = new TreeSet<ArrayInfo>(
						ArrayInfo.dataTimeComparator);
				rootDir = sysFsArrays;
				log.debug(ids + " read arrays from '" + rootDir + "'");
				dirs = rootDir.listFiles();
				for (int j = 0; j < dirs.length; j++) {
					if (!dirs[j].isDirectory())
						continue;
					int id = fromHex(dirs[j].getName());
					ArrayInfo info = fetchArrayInfo(id);
					if (info != null)
						arr.add(info);
				}
				for (ArrayInfo info : arr) {
					data.putArray(info);
				}

				rootDir = new File(ValueExInfo.sysFsRootString);
				dirs = rootDir.listFiles();
				for (int j = 0; j < dirs.length; j++) {
					if (!dirs[j].isDirectory())
						continue;
					long id = fromHex(dirs[j].getName());
					data.putValueEx(id, fetchValueExInfo(id));
				}

				/* parse merge information */
				rootDir = new File(data.sysFsString(), "merges");
				dirs = rootDir.listFiles();
				for (int j = 0; j < dirs.length; j++) {
					if (!dirs[j].isDirectory())
						continue;
					int id = fromHex(dirs[j].getName());
					MergeInfo mInfo = fetchMergeInfo(id);
					data.putMerge(id, mInfo);

					inferVEMergeStates(mInfo);
				}

				if (log.isDebugEnabled()) {
					log.debug(ids + "arrays=" + hexL(data.arrayIds));
					log.debug(ids + "value extents="
							+ hexL(data.valueExIds));
					log.debug(ids + "merges=" + hex(data.mergeIds));
				}
			} catch (Exception e) {
				log.error(ids + "Could not read sys fs entries for DA data: "
						+ e, e);
				throw ((e instanceof IOException) ? (IOException) e
						: new IOException(e));
			}
		}

		/**
		 * Update write ceiling, read ceiling and read / write rates. Rethrows
		 * any sys fs errors as IOExceptions
		 * 
		 * @throws IOException
		 *             if there's a problem reading the io_stats rate data from
		 *             sysfs
		 */
		private void refreshRates() throws IOException {
			try {
				String sysFsString = DAObject.sysFsRoot + hex(daId);

				List<String> lines = null;
				lines = readLines(sysFsString, "io_stats");
				if (lines == null) {
					log.error("io_stats file is null");
					return;
				}

				if (lines.size() < 7) {
					log.error("io_stats file has wrong length"
							+ " -- expected 7 lines, got " + lines.size());
					return;
				}

				// write ceiling
				writeCeiling = Long.parseLong(value(lines.get(0)))
						/ Utils.mbDouble;
				readCeiling = Long.parseLong(value(lines.get(1)))
						/ Utils.mbDouble;

				// write rate, inline and outline
				long wi = Long.parseLong(value(lines.get(3)));
				long wo = Long.parseLong(value(lines.get(4)));

				long ri = Long.parseLong(value(lines.get(5)));
				long ro = Long.parseLong(value(lines.get(6)));

				totalWritten = (wi + wo) / Utils.mbDouble;
				totalRead = (ri + ro) / Utils.mbDouble;

				recentWriteWork(ri + ro, wi + wo);

				if (isTrace) {
					synchronized (writeProgress) {
						log.debug("write progress = " + writeProgress);
					}
				}
			} catch (Exception e) {
				log.error(ids + "Could not read sys fs entry for io_stats: "
						+ e, e);
				throw ((e instanceof IOException) ? (IOException) e
						: new IOException(e));
			}
		}

		/** Utility to parse lines of the form 'param: value' to return 'value'. */
		private String value(String paramValue) {
			return paramValue.substring(paramValue.indexOf(": ") + 2);
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
		public double totalWritten() {
			return totalWritten;
		}

		@Override
		public double totalRead() {
			return totalRead;
		}

		/** Write max, that castle is considering as a threshold. */
		@Override
		public Double getReadCeiling() {
			return readCeiling;
		}

		/** Observed write rate, MB/s. */
		@Override
		public Double getReadRate() {
			synchronized (readProgress) {
				return readProgress.rate(shortTimeInterval);
			}
		}

		@Override
		public Double getReadRateLong() {
			synchronized (readProgress) {
				return readProgress.rate();
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
		public double totalMerged() {
			synchronized (mergeProgress) {
				return mergeProgress.totalWork();
			}
		}

		@Override
		public DAInfo getDAInfo() {
			return data;
		}

		/**
		 * @return null if there is no such array in cached data. Syncs sizes,
		 *         killing the object if there's an error.
		 * @see com.acunu.castle.control.DAView#getArrayInfo(int)
		 */
		@Override
		public ArrayInfo getArrayInfo(long id) {
			ArrayInfo info = data.getArray(id);
			try {
				syncArraySizes(info);
			} catch (IOException e) {
				// has been logged already ... just return null
				return null;
			}
			return info;
		}

		/**
		 * @return null if there is no such merge in cached data. Syncs sizes
		 *         (progress), killing the object if there's an error.
		 * @see com.acunu.castle.control.DAView#getMergeInfo(int)
		 */
		@Override
		public MergeInfo getMergeInfo(int id) {
			MergeInfo info = data.getMerge(id);
			try {
				syncMergeSizes(info);
			} catch (IOException e) {
				// has been logged already ... just return null
				return null;
			}
			return info;
		}

		/**
		 * @return null if there is no such value extent. Syncs sizes, killing
		 *         the object if there's an error.
		 * @see com.acunu.castle.control.DAView#getValueExInfo(int)
		 */
		@Override
		public ValueExInfo getValueExInfo(long id) {
			ValueExInfo info = data.getValueEx(id);
			try {
				syncValueExSizes(info);
			} catch (IOException e) {
				// has been logged already ... just return null
				return null;
			}
			return info;
		}

		/**
		 * Kills an array -- just removes it from the DAData.
		 * 
		 * @param id
		 *            the id of array to kill
		 */
		private void killArray(Long id) {
			data.removeArray(id);
		}

		/**
		 * Kills a Value extent -- just removes it from the DAData.
		 * 
		 * @param id
		 *            the id of the VE to kill
		 */
		private void killVE(Long id) {
			data.removeValueEx(id);
		}

		/**
		 * Remove merge info from DAData; also kill input arrays and the
		 * 'extentsToDrain'; set output arrays to 'NOT_MERGING'. If the
		 * mergeInfo does not exist in the data then this does nothing.
		 * 
		 * @param mInfo
		 *            the merge to kill
		 */
		private void killMerge(MergeInfo mInfo) {
			if (mInfo == null)
				return;

			// check we haven't killed it already
			if (!data.containsMerge(mInfo.id)) {
				log.warn(ids + "merge " + mInfo.ids
						+ " has already been killed -- do nothing");
				return;
			}

			log.info(ids + "kill merge " + mInfo.ids);

			/*
			 * remove input value extents TODO -- is this always correct? might
			 * be shared VEs in general
			 */
			if (mInfo.extentsToDrain == null) {
				mInfo.extentsToDrain = assembleVEList(mInfo.inputArrayIds);
				log.info(ids + "null extents to drain, so set to ALL = "
						+ mInfo.extentsToDrain);
			}

			// remove input arrays
			for (Long id : mInfo.inputArrayIds) {
				killArray(id);
			}

			// kill drained extents only
			for (Long id : mInfo.extentsToDrain) {
				killVE(id);
			}

			// output VE, if it exists, is no longer merging
			ValueExInfo vInfo = data.getValueEx(mInfo.outputValueExtentId);
			if (vInfo != null)
				vInfo.mergeState = MergeState.NOT_MERGING;

			// sync state of output arrays
			for (Long id : mInfo.outputArrayIds) {
				ArrayInfo info = getArrayInfo(id);
				info.mergeState = MergeState.NOT_MERGING;
			}

			// remove merge
			data.removeMerge(mInfo.id);
			log.info(ids + "removed merge " + mInfo.ids + ", now merges = "
					+ hex(data.mergeIds));
		}

		/**
		 * Update array sizes from sys fs. If there is an error reading from sys
		 * fs, the object is killed and the excetpion rethrown as an
		 * IOException. A certain number of sync errors are expected due to
		 * stale data.
		 * 
		 * @throws IOException
		 *             if anything goes wrong reading sys fs.
		 * @param info
		 *            the array info to sync
		 */
		private void syncArraySizes(ArrayInfo info) throws IOException {
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
				if (e instanceof FileNotFoundException) {
					// expected...
					log.warn(ids + " array " + info.ids + " missing -- kill");
				} else {
					log.warn(ids + "Could not sync array sizes for " + info.ids
							+ ": " + e + " -- kill");
				}
				killArray(info.id);
				throw ((e instanceof IOException) ? (IOException) e
						: new IOException(e));
			}
		}

		/**
		 * Update value extent sizes from sys fs. If there is an error reading
		 * from sys fs, the object is killed and the excetpion rethrown as an
		 * IOException. A certain number of sync errors are expected due to
		 * stale data.
		 * 
		 * @throws IOException
		 *             if anything goes wrong reading sys fs.
		 * @param info
		 *            the value extent info. Updates the size in bytes and
		 *            number of entries.
		 */
		private void syncValueExSizes(ValueExInfo info) throws IOException {
			if (info == null)
				return;

			try {
				// assemble size variables
				List<String> lines = readLines(info.sysFsFile, "size");

				// 'Total Size Bytes: <bytes>'
				String totalBytes = lines.get(0);
				info.sizeInBytes = Long.parseLong(totalBytes
						.substring(totalBytes.indexOf(": ") + 2));

				// 'Current Size Bytes: <bytes>'
				String currentBytes = lines.get(1);
				info.currentSizeInBytes = Long.parseLong(currentBytes
						.substring(currentBytes.indexOf(": ") + 2));

				// 'Entries: <entries>'
				String numEntries = lines.get(2);
				info.numEntries = Long.parseLong(numEntries
						.substring(numEntries.indexOf(": ") + 2));
			} catch (Exception e) {
				if (e instanceof FileNotFoundException) {
					log.warn(ids + " VE missing " + info.ids + " -- kill");
				} else
					log.warn(ids + "Could not sync VE size for " + info.ids
							+ ": " + e + " -- killing");
				killVE(info.id);
				throw ((e instanceof IOException) ? (IOException) e
						: new IOException(e));
			}
		}

		/**
		 * Update merge sizes from sys fs. If there is an error reading from sys
		 * fs, the object is killed and the excetpion rethrown as an
		 * IOException. A certain number of sync errors are expected due to
		 * stale data.
		 * 
		 * @throws IOException
		 *             if anything goes wrong reading sys fs.
		 */
		private void syncMergeSizes(MergeInfo info) throws IOException {
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
				if (e instanceof FileNotFoundException)
					log.warn(ids + "Merge missing " + info.ids + " -- kill");
				else
					log.warn(ids + "Could not sync merge sizes for merge "
							+ info.ids + ": " + e + " -- kill");
				killMerge(info);
				throw ((e instanceof IOException) ? (IOException) e
						: new IOException(e));
			}
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
		private ArrayInfo newArray(long id) {
			if (log.isDebugEnabled())
				log.debug(ids + "newArray A[" + hex(id) + "]");
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
			data.putArray(info);

			// ensure that all associated value extents are known about.
			if (info.valueExIds != null) {
				for (Long vId : info.valueExIds) {
					ensureVE(vId);
				}
			}
			return info;
		}

		/**
		 * Fetch and load into 'data' a new value extent info with the given id.
		 * If the ValueExtent already exists in the data, then simply return it.
		 * 
		 * @param id
		 *            the id of the value info to fetch and add.
		 * @return the ValueExInfo fetched.
		 */
		private ValueExInfo ensureVE(Long id) {
			if (id == null)
				return null;
			ValueExInfo info = data.getValueEx(id);
			try {
				if (info == null)
					info = fetchValueExInfo(id);
			} catch (IOException e) {
				log.error(ids + "newVE - unable to fetch info for VE["
						+ hex(id) + "]");
			}

			if (info != null)
				data.putValueEx(id, info);
			return info;
		}

		/**
		 * Since merge state is not persisted for value extents, infer it from
		 * the fact that they are involved in a merge.
		 * 
		 * @param mInfo
		 *            a merge info whose VEs should have their state set.
		 */
		private void inferVEMergeStates(MergeInfo mInfo) {
			// fix merge state of VEs drained by the merge
			SortedSet<Long> vIds = mInfo.extentsToDrain;
			// null means all...
			if (vIds == null) {
				vIds = assembleVEList(mInfo.inputArrayIds);
			}
			for (Long vId : vIds) {
				ValueExInfo vInfo = data.getValueEx(vId);
				vInfo.mergeState = MergeState.INPUT;
			}

			// fix output, if there is one.
			Long vId = mInfo.outputValueExtentId;
			if (vId != null) {
				ValueExInfo vInfo = data.getValueEx(vId);
				if (vInfo == null) {
					log.warn("Could not find info for output value extent VE["
							+ hex(vId) + "]");
				} else
					vInfo.mergeState = MergeState.OUTPUT;
			}
		}

		/**
		 * Assemble a list of all VEs attached to any of the given arrays.
		 * 
		 * @param aIds
		 *            the ids of arrays whose associated VEs should be
		 *            assembled.
		 * @return a sorted set of the ids of all value extents associated to
		 *         any array in aIds
		 */
		private SortedSet<Long> assembleVEList(List<Long> aIds) {
			if (aIds == null)
				return null;
			SortedSet<Long> vIds = new TreeSet<Long>();
			for (Long aId : aIds) {
				ArrayInfo aInfo = data.getArray(aId);
				vIds.addAll(aInfo.valueExIds);
			}
			return vIds;
		}

		/**
		 * Read array information from sys fs. If the object's sys fs directory
		 * does not exist, return null. Other errors are converted to
		 * IOException and thrown.
		 * 
		 * @return null if there is no sys fs directory corresponding to the
		 *         object with this id.
		 * @throws IOException
		 *             in the event of an error with sys fs
		 */
		private ArrayInfo fetchArrayInfo(long id) throws IOException {
			if (isTrace)
				log.trace(ids + " fetch A[" + hex(id) + "]");

			File dir = new File(data.sysFsString(), "arrays/" + hex(id));
			if (!dir.exists() || !dir.isDirectory()) {
				if (!dir.exists())
					log.warn(ids + " cannot fetch array A[" + hex(id)
							+ "] -- no sys fs directory '" + dir + "'");
				else if (!dir.isDirectory())
					log.warn(ids + " cannot fetch array A[" + hex(id)
							+ "] -- sys fs entry is not directory '" + dir
							+ "'");
				return null;
			}

			// read dataTime
			int dataTime = readHexInt(dir, "data_time");

			ArrayInfo info = new ArrayInfo(data.daId, id, dataTime);

			// assemble size variables
			syncArraySizes(info);

			// merge state
			String s = readLine(dir, "merge_state");
			info.setMergeState(s);

			// get list of value extents
			File veDir = new File(dir, "data_extents");
			info.valueExIds = readIdFromSubdirs(veDir);

			return info;
		}

		/**
		 * Read merge information from sys fs. If the object's sys fs directory
		 * does not exist, return null. Other errors are converted to
		 * IOException and thrown.
		 * 
		 * @return null if there is no sys fs directory corresponding to the
		 *         object with this id.
		 * @throws IOException
		 *             in the event of an error with sys fs
		 */
		private MergeInfo fetchMergeInfo(int id) throws IOException {
			MergeInfo info = new MergeInfo(data.daId, id);
			if (isTrace)
				log.trace(ids + " fetch " + info.ids);
			File dir = info.sysFsFile;

			if (!dir.exists() || !dir.isDirectory())
				return null;

			// read id list
			List<Long> inputArrays = readIdList(dir, "in_trees");
			List<Long> outputArray = readIdList(dir, "out_tree");

			info.inputArrayIds = inputArrays;
			info.outputArrayIds = outputArray;

			/* Read off progress. */
			syncMergeSizes(info);

			// value extent information
			List<Long> ids = readIdList(dir, "output_data_extent");
			if (ids.isEmpty()) {
				info.outputValueExtentId = null;
			} else {
				info.outputValueExtentId = ids.get(0);
			}
			info.extentsToDrain = readIdSet(dir, "drain_list");

			return info;
		}

		/**
		 * Read value extent info from sys fs. If the object's sys fs directory
		 * does not exist, return null. Other errors are converted to
		 * IOException and thrown.
		 * 
		 * @return null if there is no sys fs directory corresponding to the
		 *         object with this id.
		 * @throws IOException
		 *             in the event of an error with sys fs
		 */
		private ValueExInfo fetchValueExInfo(long id) throws IOException {
			ValueExInfo info = new ValueExInfo(data.daId, id);
			if (isTrace)
				log.trace(ids + " fetch " + info.ids);
			File dir = info.sysFsFile;

			if (!dir.exists() || !dir.isDirectory())
				return null;

			// assemble size variables
			syncValueExSizes(info);

			return info;
		}

		// ///////////////////////////////////////////////////////////////////////
		// Control interface
		// ///////////////////////////////////////////////////////////////////////

		/**
		 * Set an upper bound on the write rate, in MB/s. Catches all
		 * exceptions.
		 */
		@Override
		public void setWriteRate(double rateMB) {
			try {
				int r = (int) rateMB;
				if (rateMB > Integer.MAX_VALUE)
					r = Integer.MAX_VALUE;

				writeCeiling = (double) r;
				log.debug(ids + "set write rate to " + r);
				castleConnection.insert_rate_set(daId, (int) rateMB);
				log.trace(ids + "write rate set.");
			} catch (CastleException e) {
				log.error(ids + "Could not set write rate: " + e);
			}
		}

		/** Set target read bandwidth, MB/s. Catches all exceptions. */
		@Override
		public void setReadRate(double rateMB) {
			try {
				int r = (int) rateMB;
				if (rateMB > Integer.MAX_VALUE)
					r = Integer.MAX_VALUE;

				log.debug(ids + "set read rate to " + r);
				castleConnection.read_rate_set(daId, r);
				log.trace(ids + "read rate set");
			} catch (CastleException e) {
				log.error(ids + "Could not set read rate: " + e);
			}
		}

		/**
		 * Start a merge using the given merge config. Updates array info state
		 * to the correct merge state, and adds output array into the list.
		 * 
		 * @throws CastleException
		 *             if calling merge_start on {@linkplain Castle} induces an
		 *             exception.
		 */
		@Override
		public MergeInfo startMerge(MergeConfig mergeConfig)
				throws CastleException {
			try {
				log.info(ids + "start merge " + mergeConfig.toStringLine());

				// convert list to array
				long[] arrayIds = new long[mergeConfig.inputArrayIds.size()];
				for (int i = 0; i < arrayIds.length; i++) {
					arrayIds[i] = mergeConfig.inputArrayIds.get(i);
				}

				// update status of input and output arrays
				// check that all arrays exist.
				for (int i = 0; i < arrayIds.length; i++) {
					long arrayId = arrayIds[i];
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
					for (Long id : mergeConfig.extentsToDrain) {
						dataExtentsToDrain[i++] = id;
					}
				}

				if (isTrace)
					log.trace(ids + "call merge_start");
				/* WARNING: this hardcodes c_rda_type_t. */
				int mergeId = castleConnection.merge_start(arrayIds,
						dataExtentsToDrain, 0, 0, 0);
				if (isDebug)
					log.debug(ids + "merge_start returned merge id "
							+ hex(mergeId));

				// this will do the fetch and cache update for the merge.
				MergeInfo mergeInfo = fetchMergeInfo(mergeId);
				if (mergeInfo == null) {
					throw new CastleException(
							CastleError.C_ERR_MERGE_INVAL_ID,
							"Null merge info for merge " + hex(mergeId));
				} else {
					log.info(ids + "new merge=" + mergeInfo.toStringLine());
				}
				data.putMerge(mergeId, mergeInfo);

				// update input arrays
				for (Long id : mergeInfo.inputArrayIds) {
					// no size sync needed
					ArrayInfo info = data.getArray(id);
					info.mergeState = ArrayInfo.MergeState.INPUT;
				}

				// output arrays are new, so fetch and add to DAData
				for (Long id : mergeInfo.outputArrayIds) {
					ArrayInfo info = newArray(id);
					info.mergeState = ArrayInfo.MergeState.OUTPUT;
				}

				// output value extent (if any) is new,
				// so fetch and add to DAData
				Long vId = mergeInfo.outputValueExtentId;
				if (vId != null) {
					ValueExInfo vInfo = fetchValueExInfo(vId);
					log.debug(ids + "get info for output " + vInfo.ids);
					data.putValueEx(vId, vInfo);
				}
				inferVEMergeStates(mergeInfo);

				return mergeInfo;
			} catch (Exception e) {
				log.error(ids + "merge start failed: " + e);
				throw ((e instanceof CastleException) ? (CastleException) e
						: new CastleException(CastleError.C_ERR_MERGE_ERROR,
								"startMerge", e));
			}
		}

		/**
		 * Passes a work start event to castle, storing information about the
		 * work being done.
		 * 
		 * @see com.acunu.castle.control.DAControlServer#doWork(int, long)
		 * @throws CastleException
		 *             if merge_do_work fails on {@linkplain Castle}
		 */
		@Override
		public int doWork(int mergeId, long mergeUnits) throws CastleException {
			log.info(ids + "do work on M[" + hex(mergeId) + "]"
					+ ", units=" + Utils.toStringSize(mergeUnits));

			// fail early if there's no such merge.
			MergeInfo mInfo = data.getMerge(mergeId);
			if (mInfo == null)
				throw new CastleException(CastleError.C_ERR_MERGE_INVAL_ID,
						"merge " + hex(mergeId));

			// submit the work
			log.trace(ids + " call merge_do_work");
			int workId = castleConnection
					.merge_do_work(mergeId, mergeUnits);
			log.trace(ids + " castle responded with " + hex(workId));

			// construct the work
			MergeWork work = new MergeWork(mInfo, workId, mergeUnits);
			mergeWorks.put(workId, work);
			log.debug(ids + " work unit " + work.ids + " for merge "
					+ mInfo.ids);

			return workId;
		}

		/**
		 * Deal with an event from castle: new array. Ignores arrays we already
		 * know about.
		 * 
		 * @param arrayId
		 *            id of the new array.
		 */
		void handleNewArray(long arrayId) {
			// if this is the new output of a merge, then we already
			// know about it.
			if (data.containsArray(arrayId)) {
				log.debug(ids + "handle new array A[" + hex(arrayId)
						+ "] -- already known");
			} else {
				ArrayInfo info = newArray(arrayId);
				if (info != null) {
					log.info(ids + "handle new array " + info.ids
							+ ", send event to listeners");
					List<CastleListener> curListeners;
					synchronized (listeners) {
						curListeners = new ArrayList<CastleListener>(listeners);
					}
					for (CastleListener cl : curListeners) {
						try {
							cl.newArray(info);
						} catch (Exception e) {
							// catch all exceptions so that all listeners
							// get the event
							log.error(ids
									+ "Error passing newArray to client: ",
									e);
						}
					}
				} else
					log.error(ids + "handle new array A[" + hex(arrayId)
							+ "] cannot find array info in sys fs");
			}
		}

		/**
		 * Handle a Castle event corresponding to the end of a piece of merge
		 * work. Keep internal state correct, and propagate workDone event to
		 * nugget. Catches all exceptions
		 */
		void handleWorkDone(MergeWork work, boolean isMergeFinished) {
			String ids = this.ids + "handleWorkDone ";

			// derive work in MB
			double w = work.workDoneMB();
			// update progress record
			synchronized (mergeProgress) {
				mergeProgress.add(work.startTime, work.finishTime(), w);
			}

			MergeInfo mergeInfo = work.mergeInfo;
			if (mergeInfo == null) {
				log.error(ids + "cannot handle work for null merge");
				return;
			}

			// update array sizes and merge state
			log.info(ids + work.ids + ", " + mergeInfo.ids + ", workDone="
					+ Utils.toStringSize(work.workDone()) + ", duration="
					+ work.duration() + ", rate="
					+ Utils.twoPlaces.format(work.rate())
					+ (isMergeFinished ? " FINISHED" : ""));
			/*
			 * if finished, remove the merge and input arrays from the cache
			 * pre-emptively
			 */
			if (isMergeFinished) {
				killMerge(mergeInfo);
			} else {
				// sync work done.
				try {
					syncMergeSizes(mergeInfo);
					// update array sizes
					for (Long id : mergeInfo.inputArrayIds) {
						// getArray syncs
						getArrayInfo(id);
					}
					for (Long id : mergeInfo.outputArrayIds) {
						// getArray syncs
						getArrayInfo(id);
					}
				} catch (IOException e) {
					/*
					 * couldn't sync merge. It will have been killed by now,
					 * and all associated array killed or synced. No need to
					 * sync twice.
					 */
				}
			}

			log.debug(ids + "data synced.  Inform listeners");
			List<CastleListener> curListeners;
			synchronized (listeners) {
				curListeners = new ArrayList<CastleListener>(listeners);
			}
			for (CastleListener cl : curListeners) {
				try {
					cl.workDone(data.daId, work, isMergeFinished);
				} catch (Exception e) {
					// catch everything here so as to ensure all
					// listeners get the event.
					log.error(ids + "error while informing listener: " + e, e);
				}
			}
		}
	}
}

