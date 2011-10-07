package com.acunu.castle.control;

import java.util.TreeMap;

public abstract class AbstractCastleController<E extends DAListener> extends HexWriter implements CastleController {
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
	
	@Override
	public void newDA(DAInfo daInfo) {
		// make a new projection
		projectFacet(daInfo.daId);
	}

	@Override
	public void daDestroyed(int daId) {
		Integer _daId = daId;
		projections.remove(_daId);
	}

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

}