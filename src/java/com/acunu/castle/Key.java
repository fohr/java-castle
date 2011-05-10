package com.acunu.castle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * A multidimensional key. Each dimension is a byte[]. Keys are sorted
 * lexicographically. byte[0] represents infinity in a dimension.
 */
public class Key implements Comparable<Key>, Cloneable
{

	public static class MismatchedDimensionsException extends RuntimeException
	{
	}

	public static final int MAX_KEY_SIZE = 512;

	public final byte[][] key;

	public static byte[][] parseKey(String s)
	{
		if (!s.startsWith("[") || !s.endsWith("]"))
			throw new IllegalArgumentException(
					"Key format is [dim0,dim1...,dimN]. You must include the square brackets.");
		s = s.substring(1, s.length() - 1);
		// we want trailing empty strings for infinite range queries
		String[] str_key = s.split(",", -1);
		// todo keys should be byte[]
		byte[][] key = new byte[str_key.length][];
		for (int i = 0; i < str_key.length; i++)
			key[i] = str_key[i].getBytes();
		return key;
	}

	public Key(String s)
	{
		this(parseKey(s));
	}

	public Key(byte[][] key)
	{
		this.key = key;
	}

	public int getDimensions()
	{
		return key.length;
	}

	public byte[] getDimension(int i)
	{
		return key[i];
	}

	public String toString()
	{
		return Arrays.deepToString(key);
	}

	// returns -1 for this less than k, 0 for equal, 1 for greater
	// throws an exception if the dimensions aren't equal
	@Override
	public int compareTo(Key k)
	{
		if (this.getDimensions() != k.getDimensions())
			throw new MismatchedDimensionsException();
		for (int i = 0; i < this.getDimensions(); i++)
		{
			int s = compareByteArrays(this.getDimension(i), k.getDimension(i));
			if (s < 0)
				return -1;
			if (s > 0)
				return 1;
		}
		return 0;
	}

	// compares ba1 with ba2 lexicographically, treating the entries as unsigned
	public static int compareByteArrays(byte[] ba1, byte[] ba2)
	{
		for (int i = 0; i < ba1.length; i++)
		{
			if (i >= ba2.length)
				return 1;
			// why did they not give us unsigned types in Java?!?!
			int unsigned1i = ba1[i] >= 0 ? ba1[i] : ba1[i] + 256;
			int unsigned2i = ba2[i] >= 0 ? ba2[i] : ba2[i] + 256;
			if (unsigned1i < unsigned2i)
				return -1;
			if (unsigned1i > unsigned2i)
				return 1;
		}
		if (ba1.length < ba2.length)
			return -1;
		return 0;
	}

	static private native int length(byte[][] key) throws ArrayIndexOutOfBoundsException;

	public int getPackedLength() throws IOException
	{
		int length = length(key);
		if (length > MAX_KEY_SIZE)
			throw new IOException("Keys cannot be larger than " + MAX_KEY_SIZE + " bytes");

		return length;
	}

	/**
	 * A cheaper call than getPackedLength
	 * 
	 * @return a length that is greater than or equal to the packed length
	 */
	public int getApproximateLength()
	{
		return MAX_KEY_SIZE;
	}

	static private native int copy_to(byte[][] key, ByteBuffer keyBuffer, int keyOffset)
			throws ArrayIndexOutOfBoundsException;

	public int copyToBuffer(ByteBuffer keyBuffer) throws CastleException
	{
		int r = copy_to(key, keyBuffer, keyBuffer.position());
		if (r > Math.min(keyBuffer.remaining(), MAX_KEY_SIZE))
			throw new CastleException(12, "Key would not fit in bytebuffer");
		return r;
	}

	@Override
	public int hashCode()
	{
		return Arrays.deepHashCode(key);
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Key other = (Key) obj;
		if (getDimensions() != other.getDimensions())
			return false;
		for (int i = 0; i < key.length; i++)
			if (!Arrays.equals(key[i], other.key[i]))
				return false;
		return true;
	}

	@Override
	protected Key clone()
	{
		byte[][] newDims = key.clone();
		for (int i = 0; i < newDims.length; i++)
		{
			newDims[i] = key[i].clone();
		}
		return new Key(newDims);
	}
}
