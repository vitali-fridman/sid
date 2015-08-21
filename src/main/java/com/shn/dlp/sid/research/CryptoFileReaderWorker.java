package com.shn.dlp.sid.research;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shn.dlp.sid.entries.Cell;
import com.shn.dlp.sid.entries.CellLocation;
import com.shn.dlp.sid.entries.CellLocationListSerializer;
import com.shn.dlp.sid.entries.RawTerm;
import com.shn.dlp.sid.entries.RawTermSerializer;
import com.shn.dlp.sid.security.Crypter;
import com.shn.dlp.sid.util.MemoryInfo;

public class CryptoFileReaderWorker implements Runnable {

	public final static String MAP_NAME = "CellMap";
	public final static String DB_NAME = "SidDmap";
	private final static int CELL_LIST_SIZE = 1000;
	private final static long GIG = 1024*1024*1024l;
	private final static long  MEG = 1024*1024L;
	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());  
	
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
		File file = new File(this.fileName + Crypter.CRYPRO_FILE_SUFFIX);
		DB db = null;
		try {
			db = createDB(this.dbDirectoryName, this.shardNumber);
		} catch (IOException e) {
			LOG.error("Error creating db for shard# " + this.shardNumber); 
			LOG.error(e.getMessage());
			return;
		}
		HTreeMap<RawTerm, ArrayList<CellLocation>> dbmap = createMap(db);
		try {
			readData(file, this.numberOfShards, this.shardNumber, db, dbmap);
		} catch (IOException e) {
			LOG.error("Error creating index for shard# " + this.shardNumber);
			LOG.error(e.getMessage());
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
					allocateStartSize(100*MEG).
					allocateIncrement(100*MEG).
					metricsEnable().
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
				Cell cell = Cell.read(dis, Crypter.MAC_LENGTH);
				i++;
				if (i%5000000 == 0) {
					LOG.info("Shard: " + this.shardNumber + ". Processing cell# " + String.format("%,d", i));
//					if (i%25000000 == 0) {
//						LOG.info("Available memory: " + String.format("%,d",MemoryInfo.getAvailableMemoryNoGc()/MEG) + "MB");
//					}
				}
				RawTerm rt = new RawTerm(cell.getTerm());
				int hash = rt.hashCode();
				int positiveHash = (hash <0 ? -hash : hash);
				int shardIndex = positiveHash / (Integer.MAX_VALUE / numberOfShards);
				if (shardIndex == shardNumber) {
					CellLocation cellLocation = new CellLocation(cell.getRow(), cell.getColumn());
					ArrayList<CellLocation> list = dbmap.get(rt);
					if (list == null) {
						ArrayList<CellLocation> newList = new ArrayList<CellLocation>(CELL_LIST_SIZE);
						newList.add(cellLocation);
						dbmap.put(rt, newList);
					} else {
						list.add(cellLocation);
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
