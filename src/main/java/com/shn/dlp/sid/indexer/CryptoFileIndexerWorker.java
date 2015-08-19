package com.shn.dlp.sid.indexer;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.concurrent.Callable;

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
import com.shn.dlp.sid.security.Sha256Hmac;
import com.shn.dlp.sid.util.MemoryInfo;
import com.shn.dlp.sid.util.SidConfiguration;

public class CryptoFileIndexerWorker implements Callable<Boolean> {

	public final static String MAP_NAME = "CellMap";
	public final static String DB_NAME = "SidDmap";
	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());  
	
	private SidConfiguration config;
	private final String cryptoFileName;
	private final int numberOfShards;
	private final int shardNumber;
	
	public CryptoFileIndexerWorker(SidConfiguration config, String fileName, int numberOfShards, int shardNumber) {
		this.config = config;
		this.cryptoFileName = fileName;
		this.shardNumber = shardNumber;
		this.numberOfShards = numberOfShards;
	}

	@Override
	public Boolean call() {
		
		File file = new File(this.cryptoFileName);
		DB db = null;
		try {
			db = createDB(this.config.getIndexerTempDirectory(), this.shardNumber);
		} catch (IOException e) {
			LOG.error("Error creating db for shard# " + this.shardNumber); 
			LOG.error(e.getMessage());
			return false;
		}
		HTreeMap<RawTerm, ArrayList<CellLocation>> dbmap = createMap(db);
		try {
			readData(file, this.numberOfShards, this.shardNumber, db, dbmap);
		} catch (IOException e) {
			LOG.error("Error creating index for shard# " + this.shardNumber);
			LOG.error(e.getMessage());
			return false;
		} catch (InterruptedException e) {
			return false;
		}
		
		db.commit();
		db.close();
		
		return true;
	}
	
	private  DB createDB(String dbDirectoryName, int shardNumber) throws IOException {
		
		File dbFile = new File(dbDirectoryName + "/" + DB_NAME + "." + shardNumber);
		FileUtils.deleteQuietly(dbFile);
		
			DB db = DBMaker.fileDB(dbFile).
					transactionDisable().
					closeOnJvmShutdown().
					fileMmapEnable().
					asyncWriteEnable().
					asyncWriteFlushDelay(config.getIndexerAsyncWriteFlushDelay()).
					asyncWriteQueueSize(config.getIndexerAsyncWriteQueueSize()).
					allocateStartSize(config.getIndexerMapDbSizeIncrement()).
					allocateIncrement(config.getIndexerMapDbSizeIncrement()).
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
			DB db, HTreeMap<RawTerm, ArrayList<CellLocation>> dbmap) throws IOException, InterruptedException {

		DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file), 10000000));
		readHeader(dis);
		
		int i = 0;
		int loggingCellCount = config.getIndexerLoggingCellCount();
		while (true) {
			if (Thread.interrupted()) {
				LOG.warn("Indexer Worker has been interrupted and will exit");
				if (dis != null) {
					dis.close();
					throw new InterruptedException();
				}
			}
			
			try {
				Cell cell = Cell.read(dis, Sha256Hmac.MAC_LENGTH);
				i++;
				if (i%loggingCellCount == 0) {
					LOG.info("Shard: " + this.shardNumber + ". Processing cell# " + String.format("%,d", i));
				}
				RawTerm rt = new RawTerm(cell.getTerm());
				int hash = rt.hashCode();
				int positiveHash = (hash <0 ? -hash : hash);
				int shardIndex = positiveHash / (Integer.MAX_VALUE / numberOfShards);
				if (shardIndex == shardNumber) {
					CellLocation cellLocation = new CellLocation(cell.getRow(), cell.getColumn());
					ArrayList<CellLocation> list = dbmap.get(rt);
					if (list == null) {
						ArrayList<CellLocation> newList = new ArrayList<CellLocation>(config.getIndexerInitialCellListSize());
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
