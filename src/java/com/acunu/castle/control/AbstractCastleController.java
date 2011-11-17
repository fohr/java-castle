package com.acunu.castle.control;

import java.util.TreeMap;

public abstract class AbstractCastleController implements CastleController {
	protected CastleControlServer server;
	
	protected TreeMap<Integer, DAController> projections = new TreeMap<Integer, DAController>();
	
	/**
	 * Give this listener a CastleControlServer on which to operate.  Sets up an initial set of projections.
	 */
	public void setServer(CastleControlServer server) {
		this.server = server;
		if (server == null)
			return;
		for(Integer daId : server.daList()) {
			projectFacet(daId);
		}
	}

	private DAController projectFacet(int daId) {
		synchronized (projections) {
			if (!projections.containsKey(daId))
				projections.put(daId, makeFacet(daId));
			return projections.get(daId);
		}
	}
	
	protected abstract DAController makeFacet(int daId);

	// Castle Controller
	
	@Override
	public void newDA(DAInfo daInfo) {
		// make a new projection
		projectFacet(daInfo.daId);
	}

	@Override
	public void daDestroyed(int daId) {
		DAController proj;
		synchronized (projections) {
			proj = projections.remove(daId);
		}
		proj.dispose();
	}

	@Override
	public void scheduleMegaMerge(int daId) {
		projectFacet(daId).scheduleMegaMerge();
	}
	
	// Castle Listener
	
	@Override
	public void newArray(ArrayInfo arrayInfo) {
		DAController e = projectFacet(arrayInfo.daId);
		e.newArray(arrayInfo);
	}

	@Override
	public void workDone(int daId, MergeWork work,
			boolean isMergeFinished) {
		DAController e = projectFacet(daId);
		e.workDone(daId, work, isMergeFinished);
	}

	@Override
	public void dispose() {
		synchronized (projections) {
			for(DAController e : projections.values()) {
				e.dispose();
			}
		}
	}
}
