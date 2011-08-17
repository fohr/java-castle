package com.acunu.castle;

public interface IterCallback
{
	public void call(IterReply iterReply);

	public void handleError(int error);
}
