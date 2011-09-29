package com.acunu.castle.gn;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * A hash map which is invertible -- as well as the 'get' method there is a
 * 'getByValue' method.
 */
public class ReversibleHashMap<X, Y> {
	private HashMap<X, Y> fwd = new HashMap<X, Y>();
	private HashMap<Y, X> bwd = new HashMap<Y, X>();

	public ReversibleHashMap() {
	}
	
	public int size() {
		return fwd.size();
	}

	public Set<X> keySet() {
		return fwd.keySet();
	}

	public Set<Y> valueSet() {
		return bwd.keySet();
	}

	public boolean containsValue(Y y) {
		return bwd.containsKey(y);
	}

	public boolean containsKey(X x) {
		return fwd.containsKey(x);
	}

	public Y get(X k) {
		return fwd.get(k);
	}

	public X getByValue(Y y) {
		return bwd.get(y);
	}

	public void put(X k, Y v) {
		fwd.put(k, v);
		bwd.put(v, k);
	}

	public Y remove(X x) {
		Y y = fwd.remove(x);
		if (y != null) {
			bwd.remove(y);
		}
		return y;
	}

	public X removeByValue(Y y) {
		X x = bwd.remove(y);
		if (x != null) {
			fwd.remove(x);
		}
		return x;
	}
	
	public String toString() {
		return "fwd:" + stringOfHashMap(fwd) + ", bwd:" + stringOfHashMap(bwd);
	}
	
	public static <X, Y> String stringOfHashMap(HashMap<X, Y> map) {
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		for(Iterator<X> it = map.keySet().iterator(); it.hasNext();) {
			X x = it.next();
			sb.append(x + " -> " + map.get(x) + (it.hasNext() ? ", " : ""));
		}
		sb.append("]");
		return sb.toString();
	}
}
