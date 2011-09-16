package com.acunu.castle.gn;

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
	
	public String toString() { return t + "daId     : " + daId; }
}
