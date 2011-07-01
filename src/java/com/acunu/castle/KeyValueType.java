package com.acunu.castle;

public enum KeyValueType
{
	CASTLE_VALUE_TYPE_INVALID,
	CASTLE_VALUE_TYPE_INLINE,
	CASTLE_VALUE_TYPE_OUT_OF_LINE,
	CASTLE_VALUE_TYPE_INLINE_COUNTER;
	
	public static KeyValueType valueOf(int type)
	{
		switch(type)
		{
		case -1:
		case 0:
			return CASTLE_VALUE_TYPE_INVALID;
		case 1:
			return CASTLE_VALUE_TYPE_INLINE;
		case 2:
			return CASTLE_VALUE_TYPE_OUT_OF_LINE;
		case 3:
			return CASTLE_VALUE_TYPE_INLINE_COUNTER;
		default:
			throw new IllegalArgumentException("Invalid KeyValueType type: " + type);
		}
	}
}
