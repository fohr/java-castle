package com.acunu.util;

import java.util.ArrayList;
import java.util.Collection;

public class Functional
{
	public static <X, Y> Collection<Y> map(final Collection<X> in, final Function<X, Y> fn) {
		Collection<Y> out = new ArrayList<Y>(in.size());
		for (final X x : in)
			out.add(fn.evaluate(x));
		return out;
	}
}
