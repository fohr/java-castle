package com.acunu.castle;

import java.util.Arrays;
import java.util.List;

import com.acunu.castle.Castle.CastleKVList;

/**
 * The reply from Castle to an IterStartRequest or IterNextRequest. Contains a
 * (possibly empty) set of KeyValue pairs and a token that can be use to
 * retrieve the next batch of KeyValues.
 */
public class IterReply
{
	// really want unsigned int
	public final long token;
	/**
	 * May be null.
	 */
	public final List<KeyValue> elements;
	public final boolean hasNext;

	IterReply(long token, CastleKVList kvList)
	{
		this(token, kvList.kvList, kvList.hasNext);
	}

	IterReply(long token, List<KeyValue> elements, boolean hasNext)
	{
		this.token = token;
		this.elements = elements;
		this.hasNext = hasNext;
	}

	public String toString()
	{
		StringBuilder b = new StringBuilder(32);
		b.append("IterReply token=");
		b.append(token);
		b.append(" elements=");
		if (elements == null)
			b.append("null");
		else
			b.append(Arrays.toString(elements.toArray()));
		return b.toString();
	}
}