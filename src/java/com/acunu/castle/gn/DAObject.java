package com.acunu.castle.gn;

import java.util.Collection;
import java.util.Iterator;

/**
 * What all objects in a DA have in common.
 * 
 * @author andrewbyde
 */
public abstract class DAObject {
	public static String t = "   ";
	public final int daId;
	public DAObject(int daId) {
		this.daId = daId;
	}
	
	public String toString() { return t + "daId     : " + daId + "\n"; }
	
	public static int fromHex(String s) {
		return Integer.parseInt(s, 16);
	}

	public static String hex(int i) {
		return Integer.toHexString(i);
	}
	
	public static String hex(Collection<Integer> ids) {
		StringBuffer sb = new StringBuffer();
		sb.append("[");
		for(Iterator<Integer> it = ids.iterator(); it.hasNext(); ) {
			sb.append(hex(it.next()));
			if (it.hasNext())
				sb.append(", ");
		}
		sb.append("]");
		return sb.toString();
	}
	
	public static String hex(int[] ids) {
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		for(int i =0; i < ids.length; i++) {
			sb.append("" + hex(ids[i]));
			if (i < ids.length-1)
				sb.append(", ");
		}
		sb.append("]");
		return sb.toString();
	}

}
