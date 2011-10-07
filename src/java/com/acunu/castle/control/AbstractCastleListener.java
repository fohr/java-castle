package com.acunu.castle.control;

import java.util.TreeMap;

import org.apache.log4j.Logger;

public abstract class AbstractCastleListener<E extends DAListener> extends HexWriter implements CastleListener {
	protected static Logger log = Logger.getLogger(AbstractCastleListener.class);
	protected CastleView server;
	
	protected TreeMap<Integer, E> projections = new TreeMap<Integer, E>();
	
	/**
	 * Give this listener a CastleControlServer on which to operate.  Sets up an initial set of projections.
	 */
	public void setServer(CastleView server) {
		if ((server == null) || (server == this.server))
			return;
		this.server = server;
		log.info("" + server.toString());
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
		if (daInfo == null)
			return;
		log.debug("da=" + hex(daInfo.daId));
		projectFacet(daInfo.daId);
	}

	@Override
	public void daDestroyed(int daId) {
		Integer _daId = daId;
		log.debug("da=" + hex(daId));
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
