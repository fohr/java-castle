package com.acunu.castle.gn;

import com.acunu.castle.*;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;

public class CastleNuggetServer implements NuggetServer {

    private Castle castleConnection;
    private GoldenNugget nugget = null;
    public String name = "blah";

    public CastleNuggetServer() throws IOException
    {
        this.castleConnection = new Castle();
        new CastleEventsThread(this).start();
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

    private class MergeWork
    {
        public int mergeId;
        public int workId;
        public long mergeUnits;

        MergeWork(int mergeId, int workId, long mergeUnits)
        {
            this.mergeId = mergeId;
            this.workId = workId;
            this.mergeUnits = mergeUnits;
        }
    }

    private HashMap<Integer, MergeWork> mergeWorks = new HashMap<Integer, MergeWork>();

	public synchronized int doWork(int mergeId, long mergeUnits) throws CastleException
    {
        int workId = castleConnection.merge_do_work(mergeId, mergeUnits);

        MergeWork work = new MergeWork(mergeId, workId, mergeUnits);
        mergeWorks.put(workId, work);

        return workId;
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


    public synchronized void castleEvent(String s)
    {
        String[] args = new String[5];
        String[] prefixes = new String[]{"CMD=", "ARG1=0x", "ARG2=0x", "ARG3=0x", "ARG4=0x"};
        StringTokenizer tokenizer = new StringTokenizer(s, ":");
        int i;

        System.out.println("Castle Event " + s + "\n");

        /* Return if there isn't a nugget. */
        if(this.nugget == null)
        {
            System.out.println("No nugget registered.\n");
            return;
        }

        i = 0;
        while(tokenizer.hasMoreTokens() && (i<5))
        {
            args[i] = tokenizer.nextToken();
            if(!args[i].startsWith(prefixes[i]))
                throw new RuntimeException("Bad event string formatting: "+s);
            args[i] = args[i].substring(prefixes[i].length());
            i++;
        }

        if(i != 5)
            throw new RuntimeException("Bad event string formatting: "+s);

        if(args[0].equals("131"))
        {
            int arrayId = Integer.parseInt(args[2], 16);

            System.out.println("New array event, arrayId: "+arrayId);
	        nugget.newArray(getArrayInfo(arrayId));
        }
        else
        if(args[0].equals("132"))
        {
            int workId = Integer.parseInt(args[2], 16);
            int workDone = Integer.parseInt(args[3], 16);
            int isMergeFinished = Integer.parseInt(args[4], 16);

            System.out.println("Merge work done event, workId: "+workId+", workDone: "
                    +workDone+", isMergeFinished: "+isMergeFinished);
            MergeWork work = mergeWorks.remove(workId);
            if(work == null)
                throw new RuntimeException("Got event for non-started work");

            nugget.workDone(workId, (workDone != 0) ? work.mergeUnits : 0, isMergeFinished != 0);
        }
        else
        {
            throw new RuntimeException("Unknown event");
        }
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
        Thread.sleep(600000);
        ns.terminate();
        System.out.println("... done.");
    }
}