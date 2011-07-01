package com.acunu.castle;

import java.util.Iterator;

public interface PeekableIterator<E> extends Iterator<E>
{
	E peek();
}
