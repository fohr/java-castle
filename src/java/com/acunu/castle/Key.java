package com.acunu.castle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.EnumSet;

/**
 * A multidimensional key. Each dimension is a byte[]. Keys are sorted
 * lexicographically. byte[0] represents infinity in a dimension.
 */
public class Key implements Comparable<Key>, Cloneable {
	static {
		System.load("/usr/lib64/java-castle/libCastleImpl.so");
		init_jni();
	}

	private static native void init_jni();

	public enum KeyDimensionFlags {
		KEY_DIMENSION_NONE((byte) 0x0), KEY_DIMENSION_NEXT_FLAG((byte) 0x1), KEY_DIMENSION_MINUS_INFINITY_FLAG(
				(byte) 0x2), KEY_DIMENSION_PLUS_INFINITY_FLAG((byte) 0x4);

		public byte value;

		private KeyDimensionFlags(byte value) {
			this.value = value;
		}

		public static KeyDimensionFlags valueOf(byte[] keyDim) {
			if (keyDim.equals(PLUS_INF))
				return KEY_DIMENSION_PLUS_INFINITY_FLAG;
			else if (keyDim.equals(MINUS_INF))
				return KEY_DIMENSION_MINUS_INFINITY_FLAG;
			else
				return KEY_DIMENSION_NONE;
		}

		public static KeyDimensionFlags valueOf(byte value) {
			for (KeyDimensionFlags f : EnumSet.allOf(KeyDimensionFlags.class))
				if (f.value == value)
					return f;
			return null;
		}
	}

	public static class MismatchedDimensionsException extends RuntimeException {
	}

	public static final byte[] PLUS_INF = new byte[0];
	public static final byte[] MINUS_INF = new byte[0];

	/**
	 * The single-dimensioned "+\infty" key -- useful for open-ended range
	 * queries.
	 */
	public static final Key plusInfOneDim = new Key(new byte[][] { PLUS_INF });

	public static final int MAX_KEY_SIZE = 512;

	public final byte[][] key;

	@Deprecated
	public static byte[][] parseKey(String s) {
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

	@Deprecated
	public Key(String s) {
		this(parseKey(s));
	}

	public Key(byte[][] key) {
		this.key = key;
	}

	public int getDimensions() {
		return key.length;
	}

	public byte[] getDimension(int i) {
		return key[i];
	}

	public String toString() {
		return Arrays.deepToString(key);
	}

	/**
	 * Any dimensions that apparently contain ASCII will be printed as such. All
	 * other dimensions will be printed in the form 0xabcd, two hex characters
	 * per byte.
	 */
	public String toReadableString() {
		StringBuilder result = new StringBuilder();
		result.append("[");
		int remainingDims = key.length;
		for (byte[] dim : key) {
			boolean unprintable = false;
			for (byte b : dim) {
				if (b < 32 || b > 126) {
					unprintable = true;
					break;
				}
			}
			if (unprintable) {
				result.append("0x");
				for (byte b : dim) {
					result.append(String.format("%02x", b));
				}
			} else {
				result.append(new String(dim));
			}

			remainingDims--;
			if (remainingDims > 0)
				result.append(",");
		}
		result.append("]");
		return result.toString();
	}

	// returns -1 for this less than k, 0 for equal, 1 for greater
	// throws an exception if the dimensions aren't equal
	@Override
	public int compareTo(Key k) {
		return compareTo(k, 0, this.getDimensions());
	}

	public int compareTo(Key k, int offsetDimension, int countDimensions) {
		if (this.getDimensions() != k.getDimensions())
			throw new MismatchedDimensionsException();
		for (int i = offsetDimension; i < offsetDimension + countDimensions; i++) {
			int s = compareByteArrays(this.getDimension(i), k.getDimension(i));
			if (s < 0)
				return -1;
			if (s > 0)
				return 1;
		}
		return 0;
	}

	// compares ba1 with ba2 lexicographically, treating the entries as unsigned
	public static int compareByteArrays(byte[] ba1, byte[] ba2) {
		for (int i = 0; i < ba1.length; i++) {
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

	static private native int length(byte[][] key)
			throws ArrayIndexOutOfBoundsException;

	/**
	 * @return the length of the key inside castle, in bytes.
	 * @throws IOException
	 *             if castle objects to being asked for the length of the key.
	 */
	public int getPackedLength() throws IOException {
		int length = length(key);
		if (length > MAX_KEY_SIZE)
			throw new IOException("Keys cannot be larger than " + MAX_KEY_SIZE
					+ " bytes");

		return length;
	}

	/**
	 * A cheaper call than getPackedLength
	 * 
	 * @return a length that is greater than or equal to the packed length
	 */
	public int getApproximateLength() {
		return MAX_KEY_SIZE;
	}

	public int copyToBuffer(ByteBuffer keyBuffer) throws CastleException {
		try {
			ByteBuffer buf = keyBuffer.slice();
			buf.order(ByteOrder.LITTLE_ENDIAN);

			buf.putInt(0); /* length placeholder */
			buf.putInt(key.length); /* num dimensions */
			buf.putLong(0L); /* unused */

			int offset = 16 + 4 * key.length;
			for (int i = 0; i < key.length; i++) {
				byte flag = KeyDimensionFlags.valueOf(key[i]).value;
				int hdr = offset << 8 | flag & 0xFF;
				buf.putInt(hdr); /* dimension header */
				offset += key[i].length;
			}
			for (byte[] dim : key)
				buf.put(dim);
			int length = buf.position();
			buf.rewind();
			buf.putInt(length - 4); /* length doesn't include length field */
			return length;
		} catch (RuntimeException e) {
			throw new CastleException(-5, "Failed to copy key to buffer: "
					+ e.getMessage(), e);
		}
	}

	@Override
	public int hashCode() {
		return Arrays.deepHashCode(key);
	}

	@Override
	public boolean equals(Object obj) {
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
	public Key clone() {
		byte[][] newDims = key.clone();
		for (int i = 0; i < newDims.length; i++) {
			newDims[i] = key[i].clone();
		}
		return new Key(newDims);
	}

	public Key extend(byte[]... extraDims) {
		final byte[][] dims = new byte[key.length + extraDims.length][];
		for (int i = 0; i < key.length; ++i)
			dims[i] = key[i].clone();
		for (int i = 0; i < extraDims.length; ++i)
			dims[i + key.length] = extraDims[i].clone();
		return new Key(dims);
	}
}
