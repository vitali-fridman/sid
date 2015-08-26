package com.shn.dlp.sid.indexer;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shn.dlp.sid.entries.Cell;
import com.shn.dlp.sid.entries.CellRowAndColMask;
import com.shn.dlp.sid.entries.CellRowAndColMaskListSerializer;
import com.shn.dlp.sid.entries.RawTerm;
import com.shn.dlp.sid.entries.RawTermSerializer;
import com.shn.dlp.sid.entries.TermAndRow;
import com.shn.dlp.sid.security.CryptoException;
import com.shn.dlp.sid.util.SidConfiguration;

public class CryptoFileIndexerWorker implements Callable<Boolean> {

	
	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());  
	
	private SidConfiguration config;
	private final String cryptoFileName;
	private final int numberOfShards;
	private final int shardNumber;
	private final static String UNCOMMON_TERMS_MAP_NAME = "UncommonTermsMap";
	private final static String UNCOMMON_TERMS_DB_NAME = "UncommonTermsDB";
	private final static String ALL_COMMON_TERMS_MAP_NAME = "AllCommonTermsMap";
	private final static String ALL_COMMON_TERMS_DB_NAME = "AllConmmonTermsDB";
	private DB uncommonTermsDB;
	private HTreeMap<RawTerm, ArrayList<CellRowAndColMask>> unCommonTermsMap;
	private DB allCommonTermsDB;
	private HTreeMap<RawTerm, Integer> allCommonTermsMap;
	private final HTreeMap<TermAndRow, Integer> commonTermsMap;
	
	
	public CryptoFileIndexerWorker(SidConfiguration config, String fileName, 
			int numberOfShards, HTreeMap<TermAndRow, Integer> commonTermsMap, int shardNumber) {
		this.config = config;
		this.cryptoFileName = fileName;
		this.shardNumber = shardNumber;
		this.numberOfShards = numberOfShards;
		this.commonTermsMap = commonTermsMap;
	}

	@Override
	public Boolean call() {
		
		File file = new File(this.cryptoFileName);
		this.uncommonTermsDB = null;
		try {
			createDB(this.config.getIndexerTempDirectory(), this.shardNumber);
		} catch (IOException e) {
			LOG.error("Error creating db for shard# " + this.shardNumber); 
			LOG.error(e.getMessage());
			return false;
		}
		
		try {
			createUncommonTermsMap();
			createAllCommonTermsMap();
		} catch (CryptoException e) {
			LOG.error("Error creating dbmap", e);
			return false;
		}
		try {
			readCryptoData(file, this.numberOfShards, this.shardNumber);
			moveCommonTerms();
			LOG.info("For shard# " + this.shardNumber + " " + "found " + this.allCommonTermsMap.size() + " common terms");
		} catch (IOException e) {
			LOG.error("Error creating index for shard# " + this.shardNumber);
			LOG.error(e.getMessage());
			return false;
		} catch (InterruptedException e) {
			return false;
		}
		
		this.uncommonTermsDB.commit();
		this.uncommonTermsDB.close();
		this.allCommonTermsDB.commit();
		this.allCommonTermsDB.close();
		
		return true; 
	}
	
	private void moveCommonTerms() throws InterruptedException {
		int commonalityThreashold = this.config.getCommonalityThreashold();
		int i=0;
		for (Entry<RawTerm, ArrayList<CellRowAndColMask>> entry : this.unCommonTermsMap.entrySet()) {
			
			if (Thread.interrupted()) {
				LOG.warn("Indexer Worker has been interrupted and will exit");
					throw new InterruptedException();
			}
			
			RawTerm term = entry.getKey();
			ArrayList<CellRowAndColMask> locations = entry.getValue();
			if (locations.size() > commonalityThreashold) {
				if (i%1000 == 0) {
					LOG.info("Shard# " + this.shardNumber + " moving common term# " + i);
				}
				int combinedMask = 0;
				for (CellRowAndColMask location : locations) {
					combinedMask = combinedMask | (1 << location.getMask());
					this.commonTermsMap.put(new TermAndRow(term, location.getRow()), location.getMask());
				}
				this.allCommonTermsMap.put(entry.getKey(), combinedMask);
				this.unCommonTermsMap.remove(term);
				i++;
			}
		}
	}
	
	private  void createDB(String dbDirectoryName, int shardNumber) throws IOException {
		
		File uncommonTermsDBfile = new File(dbDirectoryName + "/" + UNCOMMON_TERMS_DB_NAME + "." + shardNumber);
		FileUtils.deleteQuietly(uncommonTermsDBfile);
		File allCommonTermsDBfile = new File(dbDirectoryName + "/" + ALL_COMMON_TERMS_DB_NAME + "." + shardNumber);
		FileUtils.deleteQuietly(allCommonTermsDBfile);
		
			this.uncommonTermsDB = DBMaker.fileDB(uncommonTermsDBfile).
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
			
			this.allCommonTermsDB = DBMaker.fileDB(allCommonTermsDBfile).
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
	}
	
	private void  createUncommonTermsMap() throws CryptoException {
		this.unCommonTermsMap =
		this.uncommonTermsDB.hashMapCreate(UNCOMMON_TERMS_MAP_NAME).
			valueSerializer(new CellRowAndColMaskListSerializer()).
			keySerializer(new RawTermSerializer(this.config)).
			counterEnable().					
			make();		
	}
	
	private void  createAllCommonTermsMap() throws CryptoException {
		this.allCommonTermsMap =
		this.allCommonTermsDB.hashMapCreate(ALL_COMMON_TERMS_MAP_NAME).
			valueSerializer(Serializer.INTEGER).
			keySerializer(new RawTermSerializer(this.config)).
			counterEnable().					
			make();		
	}

	private  void readCryptoData(File file, int numberOfShards, int shardNumber) throws IOException, InterruptedException {

		DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file), 10000000));
		int termLength = readHeader(dis);
		int initialCellListSize = config.getIndexerInitialCellListSize();
		
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
				Cell cell = Cell.read(dis, termLength);
				i++;
				if (i%loggingCellCount == 0) {
					LOG.info("Shard: " + this.shardNumber + ". Processing cell# " + String.format("%,d", i));
				}
				RawTerm rt = new RawTerm(cell.getTerm());
				int hash = rt.hashCode();
				int positiveHash = (hash <0 ? -hash : hash);
				int shardIndex = positiveHash / (Integer.MAX_VALUE / numberOfShards);
				if (shardIndex == shardNumber) {
					CellRowAndColMask newEntry = new CellRowAndColMask(cell.getRow(), cell.getColumn());
					ArrayList<CellRowAndColMask> list = this.unCommonTermsMap.get(rt);
					if (list == null) {
						ArrayList<CellRowAndColMask> newList = 
								new ArrayList<CellRowAndColMask>(initialCellListSize);
						newList.add(newEntry);
						this.unCommonTermsMap.put(rt, newList);
					} else {
						int index = Collections.binarySearch(list, newEntry);
						if (index >= 0) {
							CellRowAndColMask existngEntry = list.get(index);
							existngEntry.addToMask(cell.getColumn());
						} else {
							list.add(-index - 1, newEntry);
						}		
						this.unCommonTermsMap.put(rt, list);
					}
				}
			} catch (IOException e) {
				dis.close();	
				return;
			} 
		}
	}

	private int readHeader(DataInputStream dis) throws IOException {
		int headerLength = dis.readByte(); // unused
		int version = dis.readByte();	   // unused
		byte[] alg = new byte[this.config.getCryptoFileHeaderAlgoritmNameLength()];
		dis.readFully(alg);
		int termLength = dis.readByte();;
		dis.skip(5);
		return termLength;
	}
}
