package com.acunu.castle;

import java.util.Arrays;
import java.util.List;

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

	IterReply(long token, List<KeyValue> elements)
	{
		this.token = token;
		this.elements = elements;
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