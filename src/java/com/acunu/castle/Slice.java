package com.acunu.castle;

/**
 * Represents a hypercube in the keyspace as defined by two keys.
 */
public class Slice
{
	public final Key minKey;
	public final Key maxKey;

	public Slice(Key minKey, Key maxKey)
	{
		this.minKey = minKey;
		this.maxKey = maxKey;
	}
}