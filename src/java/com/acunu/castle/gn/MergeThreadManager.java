package com.acunu.castle.gn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class to manage the merge-thread binding.
 * 
 * @author andrewbyde
 */
public class MergeThreadManager {
	public static void error(String string) {
		System.out.println("ThreadManager :: " + string);
	}

	private List<Integer> validThreads = new ArrayList<Integer>();
	private ReversibleHashMap<Integer, Integer> threadToMergeMap = new ReversibleHashMap<Integer, Integer>();
	private CastleNuggetServer server;

	public MergeThreadManager(CastleNuggetServer server) {
		this.server = server;
	}

	/**
	 * Create a new thread and add it to the management pool.
	 */
	private int addThread() throws IOException {
		synchronized (CastleNuggetServer.syncLock) {
			int threadId = server.mergeThreadCreate();
			validThreads.add(threadId);
			return threadId;
		}
	}

	/**
	 * Get the next free thread, creating a new one if necessary.
	 * 
	 * @return null if there's an exception trying to create a new thread.
	 */
	private Integer nextFreeThread() throws IOException {
		synchronized (CastleNuggetServer.syncLock) {
			for (Integer t : validThreads) {
				if (!threadToMergeMap.containsKey(t))
					return t;
			}

			// no free ones -- so try to create a new one.
			return addThread();
		}
	}

	/**
	 * Get a thread for the given merge. If there is no thread associated, bind
	 * a new one (attach on the server).
	 */
	public void ensureThreadForMerge(int daId, int mergeId) {
		synchronized (CastleNuggetServer.syncLock) {
			Integer t = threadToMergeMap.getByValue(mergeId);
			if (t == null) {
				try {
					// get free thread, assign
					t = nextFreeThread();
					threadToMergeMap.put(t, mergeId);
					server.mergeThreadAttach(daId, mergeId, t);
				} catch (IOException e) {
					// this will happen if the merge is already
					// attached, for example
					error(e.getMessage());
					error("Suspected merge already attached");
				}
			}
		}
	}

	/**
	 * Unbind a merge. This may be called even after the merge has been removed:
	 * if we sent more work units to the server than were needed, we'll get
	 * multiple 'merge finished' messages, with 0 work done. So ignore if the
	 * merge is unknown.
	 */
	public void unbind(int mergeId) {
		synchronized (CastleNuggetServer.syncLock) {
			if (!threadToMergeMap.containsValue(mergeId)) {
				// just return
				return;
			}
			threadToMergeMap.removeByValue(mergeId);
		}
	}

	public void shutDown() {
		try {
			for (Integer id : validThreads) {
				server.mergeThreadDestroy(id);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("threads: " + validThreads + "\n");
		for (Integer tid : validThreads) {
			sb.append("  " + DAObject.hex(tid));
			if (threadToMergeMap.containsKey(tid))
				sb.append(" -> merge="
						+ DAObject.hex(threadToMergeMap.get(tid)) + "\n");
			else
				sb.append(" unbound\n");
		}
		return sb.toString();
	}
}
