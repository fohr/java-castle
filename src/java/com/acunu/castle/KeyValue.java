package com.acunu.castle;

import java.util.Arrays;

/**
 * A pair of (Castle) Key and byte[] value. There is possibly no value
 * associated with the key, in which case hasValue() is false, and getValue()
 * will throw an exception.
 */
public class KeyValue
{
	private final Key key;
	private byte[] value;
	private long valueLength;
	private boolean hasValue;

	public KeyValue(Key key)
	{
		this.key = key;
		this.value = null;
		this.valueLength = 0;

		hasValue = false;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + (hasValue ? 1231 : 1237);
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + Arrays.hashCode(value);
		result = prime * result + (int) (valueLength ^ (valueLength >>> 32));
		return result;
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
		KeyValue other = (KeyValue) obj;
		if (hasValue != other.hasValue)
			return false;
		if (key == null)
		{
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (!Arrays.equals(value, other.value))
			return false;
		if (valueLength != other.valueLength)
			return false;
		return true;
	}

	public KeyValue(Key key, byte[] value, long valueLength)
	{
		this.key = key;
		setValue(value, valueLength);
	}

	public String toString()
	{
		String s_key = key.toString();
		String s_val;
		if (value != null)
			s_val = Arrays.toString(value);
		else
			s_val = "null value";
		return s_key + " -> " + s_val;
	}

	public Key getKey()
	{
		return key;
	}

	public byte[] getValue()
	{
		if (!hasValue)
			throw new IllegalStateException("Tried to getValue() for a KeyValue which has no value");
		return value;
	}

	public boolean hasValue()
	{
		return hasValue;
	}

	public boolean hasCompleteValue()
	{
		if (!hasValue)
			return false;
		return value.length == valueLength;
	}

	protected void setValue(byte[] value, long valueLength)
	{
		if (value != null && value.length > valueLength)
			throw new IllegalArgumentException("valueLength cannot be less than value.length");
		this.value = value;
		this.valueLength = valueLength;
		this.hasValue = true;
	}

	public long getValueLength()
	{
		if (!hasValue)
			throw new IllegalStateException();
		return valueLength;
	}
}