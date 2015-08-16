package com.shn.dlp.sid.research;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.HTreeMap;

import com.shn.dlp.sid.entries.Cell;
import com.shn.dlp.sid.entries.CellLocation;
import com.shn.dlp.sid.entries.CellLocationListSerializer;
import com.shn.dlp.sid.entries.RawTerm;
import com.shn.dlp.sid.entries.RawTermSerializer;
import com.shn.dlp.sid.security.Sha256Hmac;

public class CryptoFileReader {

	public final static String MAP_NAME = "CellMap";
	public final static String DB_NAME = "SidDmap";
	private final static int CELL_LIST_SIZE = 1000;
	
	@Option(name="-f",usage="File Name to read", required=true)
	private String fileName;
	@Option(name="-d",usage="Database directory name", required=true)
	private String dbDirectoryName;
	@Option (name="-n",usage="Shard number", required=true)
	private int shardNumber;
	@Option (name="-s",usage="Number of shards", required=true)
	private int numberOfShards;
	
	public static void main(String[] args) throws IOException {
		
		// Scanner in = new Scanner(System.in);
		// in.nextLine();
		 
		CryptoFileReader cfr = new CryptoFileReader();
		CmdLineParser parser = new CmdLineParser(cfr);
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			return;
		}
		
		long start=System.nanoTime();
		
		File file = new File(cfr.fileName + Sha256Hmac.CRYPRO_FILE_SUFFIX);
		// FileUtils.deleteQuietly(new File(cfr.dbDirectoryName));
		// FileUtils.forceMkdir(new File(cfr.dbDirectoryName));
		DB db = createDB(cfr.dbDirectoryName, cfr.shardNumber);
		HTreeMap<RawTerm, ArrayList<CellLocation>> dbmap = createMap(db);
		readData(file, cfr.numberOfShards, cfr.shardNumber, db, dbmap);
		
		db.commit();
		db.close();
		
		db = null;
		dbmap = null;
		System.gc();
		System.gc();
		System.gc();
		
		db = openDB(cfr.dbDirectoryName, cfr.shardNumber);
		dbmap = openMap(db);
		inspectDBmap(dbmap);
		
		db.close();
		
		long end = System.nanoTime();
		System.out.println("Time: " + (end - start)/1000000000d + " sec");
	}

	private static HTreeMap<RawTerm, ArrayList<CellLocation>> openMap(DB db) {
		HTreeMap<RawTerm, ArrayList<CellLocation>> map = db.hashMap(MAP_NAME, 
				new RawTermSerializer(), 
				new CellLocationListSerializer(), 
				new Fun.Function1<ArrayList<CellLocation>, RawTerm>() {
			@Override
			public ArrayList<CellLocation> run(RawTerm a) {
				ArrayList<CellLocation> locations = new ArrayList<CellLocation>();
				locations.add(new CellLocation(-1, -1));
				return locations;
			}
		});
		
		return map;
	}

	private static DB openDB(String dbDirectoryName, int shardNumber) {
		File dbFile = new File(dbDirectoryName + "/" + DB_NAME + "." + shardNumber);
		DB db = DBMaker.fileDB(dbFile).readOnly().make();
		return db;
	}

	private static DB createDB(String dbDirectoryName, int shardNumber) throws IOException {
		if (shardNumber == 0) {
			FileUtils.deleteQuietly(new File(dbDirectoryName));
			FileUtils.forceMkdir(new File(dbDirectoryName));
		}
		File dbFile = new File(dbDirectoryName + "/" + DB_NAME + "." + shardNumber);
		FileUtils.deleteQuietly(dbFile);
		
			DB db = DBMaker.fileDB(dbFile).
					transactionDisable().
					closeOnJvmShutdown().
					fileMmapEnable().
					asyncWriteEnable().
					asyncWriteFlushDelay(60000).
					asyncWriteQueueSize(100000).
					allocateStartSize(1*1024*1024*1024). 
					// allocateRecidReuseDisable().
					// cacheSize(10000000).
					// cacheSoftRefEnable().
					// executorEnable().
					// metricsEnable().
					make();
		return db;
	}

	private static void inspectDBmap(HTreeMap<RawTerm, ArrayList<CellLocation>> dbmap) {
		Set<Entry<RawTerm, ArrayList<CellLocation>>> entrySet = dbmap.entrySet();
		for (Entry<RawTerm, ArrayList<CellLocation>> entry : entrySet) {
			System.out.println("Term: " + entry.getKey().toString());
			for (CellLocation cellLocation : entry.getValue()) {
				System.out.println("\t" + cellLocation.getRow() + ":" + cellLocation.getColumn());
			}
		}
	}

	private static HTreeMap<RawTerm, ArrayList<CellLocation>> createMap(DB db) {
		HTreeMap<RawTerm, ArrayList<CellLocation>> map =
		db.hashMapCreate(MAP_NAME).
			valueSerializer(new CellLocationListSerializer()).
			keySerializer(new RawTermSerializer()).
			counterEnable().					
			make();
		return map;
		
	}

	private static void readData(File file, int numberOfShards, int shardNumber,
			DB db, HTreeMap<RawTerm, ArrayList<CellLocation>> dbmap) throws IOException {

		DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file), 10000000));
		readHeader(dis);
		
		int i = 0;
		while (true) {
			try {
				Cell cell = Cell.read(dis, Sha256Hmac.MAC_LENGTH);
				i++;
				if (i%100000 == 0) {
					System.out.format("Processing cell# %,d\n", i);
				}
				// System.out.println("Cell " + cell.getRow() + ":" + cell.getColumn());
				RawTerm rt = new RawTerm(cell.getTerm());
				int hash = rt.hashCode();
				int positiveHash = (hash <0 ? -hash : hash);
				int shardIndex = positiveHash / (Integer.MAX_VALUE / numberOfShards);
				if (shardIndex == shardNumber) {
					// System.out.println("Adding cell: " + cell.getRow() + "/" + cell.getColumn() + " to shard: " + shardNumber);
					CellLocation cellLocation = new CellLocation(cell.getRow(), cell.getColumn());
					ArrayList<CellLocation> list = dbmap.get(rt);
					if (list == null) {
						ArrayList<CellLocation> newList = new ArrayList<CellLocation>(CELL_LIST_SIZE);
						newList.add(cellLocation);
						// newList.trimToSize();
						dbmap.put(rt, newList);
					} else {
						list.add(cellLocation);
						// list.trimToSize();
						dbmap.put(rt, list);
					}
				}
			} catch (IOException e) {
				return;
			}
		}

	}

	private static void readHeader(DataInputStream dis) throws IOException {
		dis.skip(6);
	}

}
