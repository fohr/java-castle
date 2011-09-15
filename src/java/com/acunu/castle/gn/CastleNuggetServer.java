package com.acunu.castle.gn;

import com.acunu.castle.*;
import java.io.IOException;

public class CastleNuggetServer implements NuggetServer {

    private Castle castleConnection;

    public CastleNuggetServer() throws IOException
    {
        this.castleConnection = new Castle();
    }

	public int setWriteRate(double rateMB)
    {
        throw new RuntimeException("not implemented yet");
    }

	/** Set target read bandwidth, MB/s. */
	public int setReadRate(double rateMB)
    {
        throw new RuntimeException("not implemented yet");
    }

	/** Observed write rate, MB/s. */
	public double getWriteRate()
    {
        throw new RuntimeException("not implemented yet");
    }

	public CastleInfo getCastleInfo()
    {
        throw new RuntimeException("not implemented yet");
    }

	public ArrayInfo getArrayInfo(int aid)
    {
        throw new RuntimeException("not implemented yet");
    }

	public MergeInfo getMergeInfo(int mid)
    {
        throw new RuntimeException("not implemented yet");
    }

	public ValueExInfo getValueExInfo(int vxid)
    {
        throw new RuntimeException("not implemented yet");
    }

	public MergeInfo startMerge(MergeConfig mergeConfig)
    {
        throw new RuntimeException("not implemented yet");
    }

	public int doWork(int mergeId, long workInBytes)
    {
        throw new RuntimeException("not implemented yet");
    }

	public void setGoldenNugget(GoldenNugget nugget)
    {
        throw new RuntimeException("not implemented yet");
    }

	public int mergeThreadCreate()
    {
        throw new RuntimeException("not implemented yet");
    }

	public void mergeThreadAttach(int mergeId, int threadId)
    {
        throw new RuntimeException("not implemented yet");
    }

	public void mergeThreadDestroy(int threadId)
    {
        throw new RuntimeException("not implemented yet");
    }

    public void terminate() throws IOException
    {
        castleConnection.disconnect();
    }

    public static void main(String[] args) throws Exception
    {
        CastleNuggetServer ns;

        System.out.println("CastleNuggetServer test ...");
        ns = new CastleNuggetServer();
        ns.terminate();
        System.out.println("... done.");
    }
}
