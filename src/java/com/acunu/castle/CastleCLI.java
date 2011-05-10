package com.acunu.castle;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CastleCLI {
	
	private static Castle castle = null;
	
	private static void do_cmd(String cmd, String[] args) {
		try {
			Class<CastleCLI> c = CastleCLI.class;
			Method m = c.getMethod(String.format("do_%s", cmd), new Class[] {String[].class});
			m.invoke(null, new Object[] {args}); // expect it to be static
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void do_interactive() throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
	    String line;

	    // print list of commands
	    System.out.println("Interactive mode");
	    System.out.print("Valid commands:");
		Class<CastleCLI> c = CastleCLI.class;
	    Method[] methods = c.getDeclaredMethods();
	    List<String> methodNames = new ArrayList<String>();
	    for (Method method: methods) {
	    	methodNames.add(method.getName());
	    }
	    Collections.sort(methodNames);
	    for (String name: methodNames) {
	    	if (name.equals("main") || name.equals("do_cmd") || name.equals("do_interactive"))
	    		continue;
	    	if (name.startsWith("do_"))
	    		System.out.print(" " + name.substring(3));
	    }
	    System.out.println("");
	    
	    System.out.print("> ");
		while ((line = br.readLine()) != null) {
			String[] args = line.split(" ");
			String cmd = args[0];
			String[] params = new String[args.length - 1];
			for (int i = 0; i < params.length; i++)
				params[i] = args[i + 1];
				
			do_cmd(cmd, params);
		    System.out.print("> ");
		}
	}
	
	public static void main(String[] args) throws IOException
	{
		// connect to localhost by default
		castle = new Castle();
		
		if (args.length == 0) {
			do_interactive();
			return;
		}
		
		String cmd = args[0];
		String[] params = new String[args.length - 1];
		for (int i = 0; i < params.length; i++)
			params[i] = args[i + 1];
		
		do_cmd(cmd, params);
	}
	
	public static void do_connect(String[] args) throws IOException {
		assert args.length == 1;
		
		castle = new Castle();
	}
	
	public static void do_help(String[] args) throws IOException {
		System.out.println("Commands:");
		System.out.println("    help - display this helpful help");
		System.out.println("    connect <host> - attach to a host (localhost?)");
		System.out.println("    put <collection> <key> <value> - put key, value:");
		System.out.println("    get <collection> <key> - get key:");
		System.out.println("    delete <collection> <key> - delete the key");
		System.out.println("    getmulti <collection> <key_1>...<key_n> - get keys key_1...key_n:");
		System.out.println("    getslice <collection> <start_key> <finish_key> <limit> - get a slice from start_key to finish_key:");
		System.out.println("    iterstart <collection> <start_key> <finish_key> - start iterator from start_key to _finish_key");
		System.out.println("    iternext <token> <batch_size> - get the next value(s)");
		System.out.println("    iterreplacelast <token> <index> <value> - replace the value at index index");
		System.out.println("    iterfinish <token> - close an iterator early");
		System.out.println("");
		System.out.println("    <collection> must be given in decimal");
		System.out.println("    <key> is a comma-separated multidimensional key entered as [dim0,dim1...,dimN]. The ASCII value of each char is taken.");
	}
	
	public static void do_quit(String[] args) throws IOException {
		System.exit(0);
	}
	
	public static void do_replace(String[] args) throws IOException {
		do_put(args);
	}
	
	public static void do_delete(String[] args) throws IOException {
		assert args.length == 2;
		
		int collection = Integer.parseInt(args[0]);
		Key key = new Key(args[1]);
		
		// System.out.println("put key="+Arrays.toString(key.key)+" value="+Arrays.toString(value));
		System.out.println("delete key="+key);
		castle.delete(collection, key);
		System.out.println("OK");
	}
	
	public static void do_put(String[] args) throws IOException {
		assert args.length == 3;
		
		int collection = Integer.parseInt(args[0]);
		Key key = new Key(args[1]);
		byte[] value = args[2].getBytes();
		
		// System.out.println("put key="+Arrays.toString(key.key)+" value="+Arrays.toString(value));
		System.out.println("put key="+key+" value="+Arrays.toString(value));
		castle.put(collection, key, value);
		System.out.println("OK");
	}
	
	public static void do_get(String[] args) throws IOException {
		assert args.length == 2;
		
		int collection = Integer.parseInt(args[0]);
		Key key = new Key(args[1]);
		System.out.println("get key="+key);
		byte[] value = castle.get(collection, key);
		if (value != null)
			System.out.println("VALUE= "+new String(value));
		else
			System.out.println("NO SUCH KEY");
	}
	
	public static void do_getmulti(String[] args) throws IOException {
		assert args.length > 2;
		
		int collection = Integer.parseInt(args[0]);
		List<Key> keys = new LinkedList<Key>();
		for (int i=1;i<args.length;i++)
    		keys.add(new Key(args[i]));
		System.out.println("getmulti keys="+keys);
		System.out.println("returned existent keys:");
		// comparator
		int dimensions = 0;
		for (Key key : keys)
			dimensions = Math.max(dimensions, key.getDimensions());

		Map<Key,byte[]> values = castle.get_multi(collection, keys);
		
		for (Key key : values.keySet())
			System.out.println(key+","+new String(values.get(key)));
	}
	
	public static void do_getslice(String[] args) throws IOException {
		assert args.length == 4;
		
		int collection = Integer.parseInt(args[0]);
		Key start = new Key(args[1]);
		Key finish = new Key(args[2]);
		int limit = Integer.parseInt(args[3]);
		System.out.println("getslice ["+start+" -> "+finish+"] limit="+limit);
		// comparator
		assert start.getDimensions() == finish.getDimensions();

		List<KeyValue> values = castle.get_slice(collection, new Slice(start, finish), limit);
		
		System.out.println("returned existent keys:");
		for (KeyValue kv : values) {
			String s_key = kv.getKey().toString();
			String s_val = kv.hasValue() ? new String(kv.getValue()) : "<large value>";
			System.out.println(s_key + " -> " + s_val);
		}
	}
	
	public static void do_claim(String[] args) throws IOException {
		assert args.length == 1;
		
		int device_id = Integer.parseInt(args[0]);
		int slave_id = castle.claim(device_id);
		
		System.out.println(String.format("Ret val: 0x%x", slave_id));
	}
	
	public static void do_release(String[] args) throws IOException {
		assert args.length == 1;
		
		int device_id = Integer.parseInt(args[0]);
		castle.release(device_id);
		
		System.out.println("Ret val: 0x0");
	}
	
	public static void do_attach(String[] args) throws IOException {
		assert args.length == 1;
		
		int device_id = Integer.parseInt(args[0]);
		int slave_id = castle.attach(device_id);
		
		System.out.println(String.format("Ret val: 0x%x", slave_id));
	}

	public static void do_detach(String[] args) throws IOException {
		assert args.length == 1;
		
		int device_id = Integer.parseInt(args[0]);
		castle.detach(device_id);
		
		System.out.println("Ret val: 0x0\n");
	}
	
	public static void do_create(String[] args) throws IOException {
		assert args.length == 1;
		
		long size = Long.parseLong(args[0]);
		int version_id = castle.create(size);
		
		System.out.println(String.format("Ret val: 0x%x", version_id));
	}
	
	public static void do_clone(String[] args) throws IOException {
		assert args.length == 1;
		
		int version_id = Integer.parseInt(args[0]);
		int new_version_id = castle.clone(version_id);
		
		System.out.println(String.format("Ret val: 0x%x", new_version_id));
	}
	
	public static void do_snapshot(String[] args) throws IOException {
		assert args.length == 1;
		
		int device_id = Integer.parseInt(args[0]);
		int version_id = castle.snapshot(device_id);
		
		System.out.println(String.format("Ret val: 0x%x", version_id));
	}
	
	public static void do_init(String[] args) throws IOException {
		assert args.length == 0;
		
		castle.init();
		
		System.out.println("Ret val: 0x0");
	}
	
	public static void do_collection_attach(String[] args) throws IOException {
		assert args.length == 2;
		
		int version_id = Integer.parseInt(args[0]);
		String name = args[1];
		int collection_id = castle.collection_attach(version_id, name);
		
		System.out.println(String.format("Ret val: 0x%x", collection_id));
	}
	
	public static void do_collection_detach(String[] args) throws IOException {
		assert args.length == 1;
		
		int collection_id = Integer.parseInt(args[0]);
		castle.collection_detach(collection_id);
		
		System.out.println("Ret val: 0x0");
	}
	
	public static void do_collection_snapshot(String[] args) throws IOException {
		assert args.length == 1;
		
		int collection_id = Integer.parseInt(args[0]);
		int version_id = castle.collection_snapshot(collection_id);
		
		System.out.println(String.format("Ret val: 0x%x", version_id));
	}
	
	public static void do_iterstart(String[] args) throws IOException {
		assert args.length == 3;
		
		int collection_id = Integer.parseInt(args[0]);
		Key start = new Key(args[1]);
		Key finish = new Key(args[2]);
		
		IterReply iterReply = castle.iterstart(collection_id, start, finish);
		
		System.out.println(iterReply.toString());
	}
	
	public static void do_iternext(String[] args) throws IOException {
		assert args.length == 2;
		
		long token = Long.parseLong(args[0]);
		int batchSize = Integer.parseInt(args[1]);
		
		IterReply iterReply = castle.iternext(token, batchSize);
		
		System.out.println(iterReply.toString());
	}
	
	public static void do_iterreplacelast(String[] args) throws IOException {
		assert args.length == 3;
		
		int token = Integer.parseInt(args[0]);
		int index = Integer.parseInt(args[1]);
		String value = args[2];
		
		castle.iterreplacelast(token, index, value.getBytes());
		
		System.out.println("OK");
	}
	
	public static void do_iterfinish(String[] args) throws IOException {
		assert args.length == 1;
		
		int token = Integer.parseInt(args[0]);
		
		castle.iterfinish(token);
		
		System.out.println("OK");
	}
}
