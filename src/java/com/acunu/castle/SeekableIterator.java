package com.acunu.castle;

import java.io.IOException;

public interface SeekableIterator<T, I> extends CloseablePeekableIterator<T>
{
	void rollback(I rollbackKey) throws IOException;
}
