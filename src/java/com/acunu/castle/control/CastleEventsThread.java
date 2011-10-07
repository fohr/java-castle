package com.acunu.castle.control;

public class CastleEventsThread extends Thread
{
	private native void events_callback_thread_run();

    private CastleControlServerImpl nuggetServer;

    public CastleEventsThread(CastleControlServerImpl ns)
    {
        nuggetServer = ns;
    }

    public void run()
    {
        events_callback_thread_run();
    }

    private void udevEvent(final String s)
    {
        nuggetServer.castleEvent(s);
    }

    void terminate()
    {
        throw new RuntimeException("not implemented");
    }
}
