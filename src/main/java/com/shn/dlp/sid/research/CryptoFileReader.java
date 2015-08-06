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
import org.mapdb.HTreeMap;

import com.shn.dlp.sid.entries.Cell;
import com.shn.dlp.sid.entries.CellLocation;
import com.shn.dlp.sid.entries.CellLocationListSerializer;
import com.shn.dlp.sid.entries.RawTerm;
import com.shn.dlp.sid.entries.RawTermSerializer;
import com.shn.dlp.sid.security.Sha256Hmac;
import com.shn.dlp.sid.tools.TestDataFileGenerator;

public class CryptoFileReader {

	public final static String MAP_NAME = "CellMap";
	public final static String DB_NAME = "SidDmap";
	
	@Option(name="-f",usage="File Name to read", required=true)
	private String fileName;
	@Option(name="-d",usage="Database directory name", required=true)
	private String dbDirectoryName;
	@Option (name="-n",usage="Number f shards", required=true)
	private int numShards;
	
	public static void main(String[] args) throws IOException {
		
		CryptoFileReader cfr = new CryptoFileReader();
		CmdLineParser parser = new CmdLineParser(cfr);
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			return;
		}
		
		File file = new File(cfr.fileName + TestDataFileGenerator.CRYPTO_SUFFIX);
		DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
		
		readHeader(dis);
		DB[] databases = createDB(cfr.dbDirectoryName, cfr.numShards);
		
		HTreeMap<RawTerm,ArrayList<CellLocation>> shards[] = createMaps(databases, cfr.numShards);
		
		readData(dis, shards);
		
		for (int i=0; i<shards.length; i++) {
			System.out.println("Shard# " + i + " Size: " + shards[i].mappingCount());
		}
		
		dis.close();
		for (DB db : databases) {
			db.commit();
		}
		// db.compact();
		// inspectDBmap(dbmap);
		for (HTreeMap<RawTerm,ArrayList<CellLocation>> shard : shards) {
			shard.close();
		}
	}

	private static DB[] createDB(String dbDirectoryName, int numShards) throws IOException {
		FileUtils.deleteQuietly(new File(dbDirectoryName));
		FileUtils.forceMkdir(new File(dbDirectoryName));
		DB[] databases = new DB[numShards];
		for (int i=0; i<numShards; i++) {
			databases[i] = DBMaker.fileDB(new File(dbDirectoryName + "/" + DB_NAME + "." + i)).
					transactionDisable().
					// closeOnJvmShutdown().
					fileMmapEnable().
					asyncWriteEnable().
					asyncWriteFlushDelay(60000).
					asyncWriteQueueSize(100000).
					cacheSize(10000000).
					cacheSoftRefEnable().
					executorEnable().
					// _newAppendFileDB(new File(dbDirectoryName + "/" + DB_NAME + ".append" + i)).
					// metricsEnable().
					make();
		}
		return databases;
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

	private static HTreeMap<RawTerm, ArrayList<CellLocation>>[] createMaps(DB[] databases, int numShards) {
		HTreeMap<RawTerm, ArrayList<CellLocation>>[] shards = 
				(HTreeMap<RawTerm, ArrayList<CellLocation>>[]) new HTreeMap [numShards];
		for(int i=0; i<numShards; i++) {
			shards[i] = databases[i].hashMapCreate(MAP_NAME + "." + i).
					valueSerializer(new CellLocationListSerializer()).
					keySerializer(new RawTermSerializer()).
					counterEnable().
					make();
		}
		return shards;
		
	}

	private static void readData(DataInputStream dis, HTreeMap<RawTerm,ArrayList<CellLocation>>[] shards) {
		
		int numShards = shards.length;
		
		int i=0;
		long start=System.nanoTime();
		long end = 0;
		
		while(true) {
			try {
				Cell cell = Cell.read(dis, Sha256Hmac.MAC_LENGTH);
				// System.out.println("Cell " + cell.getRow() + ":" + cell.getColumn());
				if (i%100000 == 0) {
					end = System.nanoTime();
					System.out.println("Time: " + ((end-start)/1000000000d));
					start = end;
					System.out.println("Cell: " + i/1000 +"K");
					for (int j=0; j<shards.length; j++) {
						System.out.println("Srard# " + j + " Size: " + shards[j].size());
					}
				}
				CellLocation cellLocation = new CellLocation(cell.getRow(), cell.getColumn());
				
				//insert into appropriate shard
				RawTerm rt = new RawTerm(cell.getTerm());
				int hash = rt.hashCode();
				int positiveHash = (hash <0 ? -hash : hash);
				int shardIndex = positiveHash / (Integer.MAX_VALUE / numShards);
				HTreeMap<RawTerm, ArrayList<CellLocation>> shard = shards[shardIndex];
				
				// System.out.println("Inserting into shard: " + shardIndex + " Size: " + shard.mappingCount());
				
				ArrayList<CellLocation> list = shard.get(rt);
				if (list == null) {
					ArrayList<CellLocation> newList = new ArrayList<CellLocation>();
					newList.add(cellLocation);
					newList.trimToSize();
					shard.put(rt, newList);
				} else {
					list.add(cellLocation);
					list.trimToSize();
					shard.put(rt, list);
				}
				
			} catch (IOException e) {
				// end of file reached
				return;
			}
			i++;
		}
		
	}

	private static void readHeader(DataInputStream dis) throws IOException {
		dis.skip(6);
	}

}
