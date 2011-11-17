package com.acunu.castle.control;

import java.util.TreeMap;

import org.apache.log4j.Logger;

import static com.acunu.castle.control.HexWriter.*;

public abstract class AbstractCastleListener implements CastleListener {
	protected static Logger log = Logger.getLogger(AbstractCastleListener.class);
	protected CastleView server;
	
	protected TreeMap<Integer, DAListener> projections = new TreeMap<Integer, DAListener>();
	
	/**
	 * Give this listener a CastleControlServer on which to operate.  Sets up an initial set of projections.
	 */
	public void setServer(CastleView server) {
		if ((server == null) || (server == this.server))
			return;
		this.server = server;
		log.info("set server " + server.toString());
		for(Integer daId : server.daList()) {
			projectFacet(daId);
		}
	}

	private DAListener projectFacet(int daId) {
		synchronized (projections) {
			if (!projections.containsKey(daId))
				projections.put(daId, makeFacet(daId));
			return projections.get(daId);
		}
	}
	
	protected abstract DAListener makeFacet(int daId);
	
	@Override
	public void newDA(DAInfo daInfo) {
		if (daInfo == null)
			return;
		log.debug("new " + daInfo.ids);
		projectFacet(daInfo.daId);
	}

	@Override
	public void daDestroyed(int daId) {
		synchronized (projections) {
			log.debug("destroy DA[" + hex(daId) + "]");
			DAListener proj = projections.remove((Integer) daId);
			proj.dispose();
		}
	}

	@Override
	public void newArray(ArrayInfo arrayInfo) {
		DAListener e = projectFacet(arrayInfo.daId);
		e.newArray(arrayInfo);
	}

	@Override
	public void workDone(int daId, MergeWork work,
			boolean isMergeFinished) {
		DAListener e = projectFacet(daId);
		e.workDone(daId, work, isMergeFinished);
	}

	@Override
	public void dispose() {
		synchronized (projections) {
			for(DAListener e : projections.values()) {
				e.dispose();
			}
		}
	}

}
