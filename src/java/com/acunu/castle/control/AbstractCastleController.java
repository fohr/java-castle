package com.acunu.castle.control;

import java.util.TreeMap;

public abstract class AbstractCastleController<E extends DAController> extends HexWriter implements CastleController {
	protected CastleControlServer server;
	
	protected TreeMap<Integer, E> projections = new TreeMap<Integer, E>();
	
	/**
	 * Give this listener a CastleControlServer on which to operate.  Sets up an initial set of projections.
	 */
	public void setServer(CastleControlServer server) {
		this.server = server;
		if (server == null)
			return;
		synchronized(CastleControlServerImpl.syncLock) {
			for(Integer daId : server.daList()) {
				projectFacet(daId);
			}
		}
	}

	private E projectFacet(int daId) {
		Integer _daId = daId;
		if (!projections.containsKey(_daId))
			projections.put(daId, makeFacet(daId));
		return projections.get(_daId);
	}
	
	protected abstract E makeFacet(int daId);

	// Castle Controller
	
	@Override
	public void newDA(DAInfo daInfo) {
		// make a new projection
		projectFacet(daInfo.daId);
	}

	@Override
	public void daDestroyed(int daId) {
		Integer _daId = daId;
		E proj = projections.remove(_daId);
		proj.dispose();
	}

	@Override
	public void scheduleMegaMerge(int daId) {
		projectFacet(daId).scheduleMegaMerge();
	}
	
	// Castle Listener
	
	@Override
	public void newArray(ArrayInfo arrayInfo) {
		E e = projectFacet(arrayInfo.daId);
		e.newArray(arrayInfo);
	}

	@Override
	public void workDone(int daId, int workId, long workDoneBytes,
			boolean isMergeFinished) {
		E e = projectFacet(daId);
		e.workDone(daId, workId, workDoneBytes, isMergeFinished);
	}

	@Override
	public void dispose() {
		for(E e : projections.values()) {
			e.dispose();
		}
	}
}
