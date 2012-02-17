package com.acunu.castle;

public interface EventListener
{
    void udevEvent(final String s);

    void error(Throwable t);

    void started();
}
