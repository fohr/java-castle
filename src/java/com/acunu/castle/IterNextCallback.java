package com.acunu.castle;

public interface IterNextCallback
{
	public void call(IterReply iterReply);

	public void handleError(int error);
}
