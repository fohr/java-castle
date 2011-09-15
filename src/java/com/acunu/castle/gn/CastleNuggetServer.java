package com.acunu.castle.gn;

import com.acunu.castle.*;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;

public class CastleNuggetServer implements NuggetServer {

    private Castle castleConnection;
    private GoldenNugget nugget = null;
    public String name = "blah";

    public CastleNuggetServer() throws IOException
    {
        this.castleConnection = new Castle(new HashMap<Integer, Integer>(), false);
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
        List<ArrayId> arrayIds = new LinkedList<ArrayId>();
        Set<Integer> valueExIds = new HashSet<Integer>();
        List<MergeId> mergeIds = new LinkedList<MergeId>();

        File vertreesDir = new File("/sys/fs/castle-fs/vertrees");

        File[] vertrees = vertreesDir.listFiles();
        for(int i=0; i<vertrees.length; i++)
        {
            if(!vertrees[i].isDirectory())
                continue;
            System.out.println(i+" "+vertrees[i]);
            int vertreeId = Integer.parseInt(vertrees[i].getName(), 16);

            File arraysDir = new File(vertrees[i], "arrays");
            File[] arrays = arraysDir.listFiles();

            for(int j=0; j<arrays.length; j++)
            {
                if(!arrays[j].isDirectory())
                    continue;
                int arrayId = Integer.parseInt(arrays[j].getName(), 16);
                System.out.println(i+" "+vertrees[i]+" "+arrays[j]+" ("+vertreeId+", "+arrayId+")");

                arrayIds.add(new ArrayId(vertreeId, arrayId));
            }

            File mergesDir = new File(vertrees[i], "merges");
            File[] merges = mergesDir.listFiles();

            for(int j=0; j<merges.length; j++)
            {
                if(!merges[j].isDirectory())
                    continue;
                int mergeId = Integer.parseInt(merges[j].getName(), 16);
                System.out.println(i+" "+vertrees[i]+" "+merges[j]+" ("+vertreeId+", "+mergeId+")");

                mergeIds.add(new MergeId(vertreeId, mergeId));
            }
        }

        return new CastleInfo(arrayIds, valueExIds, mergeIds);
    }

	public ArrayInfo getArrayInfo(ArrayId aid) throws CastleException
    {
        ArrayInfo ai = new ArrayInfo(aid);

        try {
            String path = "/sys/fs/castle-fs/vertrees/"+Integer.toString(aid.daId, 16)+
                                    "/arrays/"+Integer.toString(aid.arrayId, 16)+"/size";
            File sizeFile = new File(path);
            BufferedReader in = new BufferedReader(new FileReader(sizeFile));
            String sizeStr = in.readLine();
            long size = Integer.parseInt(sizeStr) * 1024 * 1024;

            ai.capacityInBytes = size;
        } catch (Exception e) {
            throw new CastleException(-22, e.toString());
        }
        // TODO: sizeInBytes & isMerging need to be dealt with

        return ai;
    }

	public MergeInfo getMergeInfo(MergeId mid)
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
        MergeId mid = new MergeId(mergeConfig.daId, mergeId);

        return getMergeInfo(mid);
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
            // TODO: DA id needs to be part of the event
            // TODO: How to handle errors in reading the array better?
            try {
	            nugget.newArray(getArrayInfo(new ArrayId(-1, arrayId)));
            } catch (Exception e)
            {
            }
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
        ns.getCastleInfo();
        ArrayInfo ai = ns.getArrayInfo(new ArrayId(7, 57));
        System.out.println("Size of the array: "+ai.capacityInBytes);
        ns.terminate();
        System.out.println("... done.");
    }
}
