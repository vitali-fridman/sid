package com.shn.dlp.sid.research;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import com.shn.dlp.sid.entries.Cell;
import com.shn.dlp.sid.entries.CellLocation;
import com.shn.dlp.sid.entries.CellLocationListSerializer;
import com.shn.dlp.sid.entries.RawTerm;
import com.shn.dlp.sid.entries.RawTermSerializer;
import com.shn.dlp.sid.security.Sha256Hmac;
import com.shn.dlp.sid.tools.ZipfTestDataFileGenerator;

public class CryptoFileReaderWorker implements Runnable {

	public final static String MAP_NAME = "CellMap";
	public final static String DB_NAME = "SidDmap";
	private final static int CELL_LIST_SIZE = 1000;
	
	private final String fileName;
	private final String dbDirectoryName;
	private final int numberOfShards;
	private final int shardNumber;
	
	public CryptoFileReaderWorker(String fileName, String dbDirectoryName, int numberOfShards, int shardNumber) {
		this.fileName = fileName;
		this.dbDirectoryName = dbDirectoryName;
		this.shardNumber = shardNumber;
		this.numberOfShards = numberOfShards;
	}

	@Override
	public void run() {
		File file = new File(this.fileName + ZipfTestDataFileGenerator.CRYPTO_SUFFIX);
		DB db = null;;
		try {
			db = createDB(this.dbDirectoryName, this.shardNumber);
		} catch (IOException e) {
			System.out.println("Error creating db for shard# " + this.shardNumber);
			System.out.println(e.getMessage());
			return;
		}
		HTreeMap<RawTerm, ArrayList<CellLocation>> dbmap = createMap(db);
		try {
			readData(file, this.numberOfShards, this.shardNumber, db, dbmap);
		} catch (IOException e) {
			System.out.println("Error creating index for shard# " + this.shardNumber);
			System.out.println(e.getMessage());
			return;
		}
		
		db.commit();
		db.close();

	}
	
	private  DB createDB(String dbDirectoryName, int shardNumber) throws IOException {
		
		File dbFile = new File(dbDirectoryName + "/" + DB_NAME + "." + shardNumber);
		FileUtils.deleteQuietly(dbFile);
		
			DB db = DBMaker.fileDB(dbFile).
					transactionDisable().
					closeOnJvmShutdown().
					fileMmapEnable().
					asyncWriteEnable().
					asyncWriteFlushDelay(60000).
					asyncWriteQueueSize(100000).
					// allocateStartSize(10*1024*1024*1024).
					// allocateRecidReuseDisable().
					// cacheSize(10000000).
					// cacheSoftRefEnable().
					// executorEnable().
					// metricsEnable().
					make();
		return db;
	}
	
	private  HTreeMap<RawTerm, ArrayList<CellLocation>> createMap(DB db) {
		HTreeMap<RawTerm, ArrayList<CellLocation>> map =
		db.hashMapCreate(MAP_NAME).
			valueSerializer(new CellLocationListSerializer()).
			keySerializer(new RawTermSerializer()).
			counterEnable().					
			make();
		return map;
		
	}

	private  void readData(File file, int numberOfShards, int shardNumber,
			DB db, HTreeMap<RawTerm, ArrayList<CellLocation>> dbmap) throws IOException {

		DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file), 10000000));
		readHeader(dis);
		
		int i = 0;
		while (true) {
			try {
				Cell cell = Cell.read(dis, Sha256Hmac.MAC_LENGTH);
				i++;
				if (i%100000 == 0) {
					System.out.format("Shard: " + this.shardNumber + ". Processing cell# %,d\n", i);
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
				dis.close();
				return;
			} 
		}
	}

	private static void readHeader(DataInputStream dis) throws IOException {
		dis.skip(6);
	}
}
