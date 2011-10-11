package com.acunu.util;
import java.io.Serializable;

/**
 * Basic construct consisting of two typed objects.
 * 
 * @author andrewbyde
 */
public class Pair<A, B> implements Serializable {
	private static final long serialVersionUID = 1L;
	protected A a;
	protected B b;
	
	public Pair(A a, B b) {
		this.a = a;
		this.b = b;
	}
	public A fst() { return a; }
	public void setFst(A _a) { a = _a; }
	public B snd() { return b; }
	public void setSnd(B _b) { b = _b; }
	public String toString() { return "("+a+", "+b+")"; }
	
	public boolean equals(Object x) {
		@SuppressWarnings("unchecked")
		Pair<A, B> p = (Pair<A, B>)x;
		if (x == null) return false;
		
		A a2 = p.fst();
		if ((a == null) && (a2 != null))
			return false;

		B b2 = p.snd();
		if ((b == null) && (b2 != null))
			return false;
		
		return ((a==null || a.equals(a2)) && (b==null || b.equals(b2)));
	}
}
