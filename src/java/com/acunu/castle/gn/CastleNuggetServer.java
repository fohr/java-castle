package com.acunu.castle.gn;

import com.acunu.castle.*;
import java.io.IOException;

public class CastleNuggetServer implements NuggetServer {

    private Castle castleConnection;
    private GoldenNugget nugget = null;

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



	public MergeInfo startMerge(MergeConfig mergeConfig) throws CastleException
    {
        int[] arrayArray = new int[mergeConfig.inputArrayIds.size()];

        int idx = 0;
        for(int arrayId : arrayArray)
            arrayArray[idx++] = arrayId;

        /* WARNING: this hardcodes c_rda_type_t. */
        int mergeId = castleConnection.merge_start(arrayArray, 0, 0, 0);

        return getMergeInfo(mergeId);
    }

	public int doWork(int mergeId, long mergeUnits) throws CastleException
    {
        return castleConnection.merge_do_work(mergeId, mergeUnits);
    }



	public int mergeThreadCreate() throws CastleException
    {
        return castleConnection.merge_thread_create();
    }

	public void mergeThreadAttach(int mergeId, int threadId) throws CastleException
    {
        castleConnection.merge_thread_attach(mergeId, threadId);
    }

	public void mergeThreadDestroy(int threadId) throws CastleException
    {
        castleConnection.merge_thread_destroy(threadId);
    }



    public synchronized void setGoldenNugget(GoldenNugget nugget)
    {
        if(this.nugget != null)
            throw new RuntimeException("Cannot register nugget multiple times.");

        this.nugget = nugget;
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
