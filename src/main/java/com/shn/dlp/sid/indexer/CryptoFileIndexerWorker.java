package com.shn.dlp.sid.indexer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.BloomFilter;
import com.shn.dlp.sid.entries.Cell;
import com.shn.dlp.sid.entries.CellRowAndColMask;
import com.shn.dlp.sid.entries.CellRowAndColMaskListSerializer;
import com.shn.dlp.sid.entries.RawTerm;
import com.shn.dlp.sid.entries.RawTermFunnel;
import com.shn.dlp.sid.entries.RawTermSerializer;
import com.shn.dlp.sid.entries.TermAndRow;
import com.shn.dlp.sid.security.CryptoException;
import com.shn.dlp.sid.util.SidConfiguration;
import static com.shn.dlp.sid.indexer.IndexComponents.UNCOMMON_TERMS_DB_NAME;
import static com.shn.dlp.sid.indexer.IndexComponents.UNCOMMON_TERMS_MAP_NAME;
import static com.shn.dlp.sid.indexer.IndexComponents.COMMON_TERMS_DB_NAME;
import static com.shn.dlp.sid.indexer.IndexComponents.COMMON_TERMS_MAP_NAME;
import static com.shn.dlp.sid.indexer.IndexComponents.COMMON_TERMS_FILTER_FILE_NAME;
import static com.shn.dlp.sid.indexer.IndexComponents.UNCOMMON_TERMS_FILTER_FILE_NAME;

public class CryptoFileIndexerWorker implements Callable<Boolean> {


	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());  

	private SidConfiguration config;
	private final String cryptoFileName;
	private final int numberOfShards;
	private final int shardNumber;
	private DB uncommonTermsDB;
	private HTreeMap<RawTerm, ArrayList<CellRowAndColMask>> unCommonTermsMap;
	private DB commonTermsDB;
	private HTreeMap<RawTerm, Integer> commonTermsMap;
	private final HTreeMap<TermAndRow, Integer> commonTermsAndRowMap;


	public CryptoFileIndexerWorker(SidConfiguration config, String fileName, 
			int numberOfShards, HTreeMap<TermAndRow, Integer> commonTermsMap, int shardNumber) {
		this.config = config;
		this.cryptoFileName = fileName;
		this.shardNumber = shardNumber;
		this.numberOfShards = numberOfShards;
		this.commonTermsAndRowMap = commonTermsMap;
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
				LOG.info("For shard# " + this.shardNumber + " " + "found " + this.commonTermsMap.size() + " common terms");
			} catch (IOException e) {
				LOG.error("Error creating index for shard# " + this.shardNumber);
				LOG.error(e.getMessage());
				return false;
			} catch (InterruptedException e) {
				return false;
			}

			this.uncommonTermsDB.commit();
			this.commonTermsDB.commit();

			try {
				createFilters();
			} catch (IOException e) {
				LOG.error("Error creating filters for shard# " + this.shardNumber);
				return false;
			}
		} finally {
			this.commonTermsDB.close();
			this.uncommonTermsDB.close();
		}

		return true; 
	}

	private void createFilters() throws IOException {
		String uncommonTermsFilterFile = config.getIndexerTempDirectory() + File.separator + 
				UNCOMMON_TERMS_FILTER_FILE_NAME + "." + this.shardNumber;
		createTermsFilter(uncommonTermsFilterFile, 
				(int) this.unCommonTermsMap.mappingCount(), 
				this.unCommonTermsMap.keySet().iterator());
		String commonTermsFilterFile = config.getIndexerTempDirectory() + File.separator + 
				COMMON_TERMS_FILTER_FILE_NAME + "." + this.shardNumber;
		createTermsFilter(commonTermsFilterFile, 
				(int) this.commonTermsMap.mappingCount(), 
				this.commonTermsMap.keySet().iterator());

	}

	private void createTermsFilter(String filterFileName, int numEntries, 
			Iterator<RawTerm> termsIterator) 
					throws IOException {

		double filterFPP = this.config.getBloomFilterFPP();

		LOG.info("For " + filterFileName + " number of entries is: " + String.format("%,d", numEntries));

		BloomFilter<RawTerm> filter = BloomFilter.create(RawTermFunnel.INSTANCE, numEntries, filterFPP);

		int count = 0;
		while (termsIterator.hasNext()) {
			filter.put(termsIterator.next());
			if (count%100000 == 0) {
				LOG.info("Adding entry# " + String.format("%,d", count) + " to " + filterFileName + " Filter.");
			}
			count++;
		}

		BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(filterFileName));
		filter.writeTo(out);
		out.close();
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
					this.commonTermsAndRowMap.put(new TermAndRow(term, location.getRow()), location.getMask());
				}
				this.commonTermsMap.put(entry.getKey(), combinedMask);
				this.unCommonTermsMap.remove(term);
				i++;
			}
		}
	}

	private  void createDB(String dbDirectoryName, int shardNumber) throws IOException {

		File uncommonTermsDBfile = new File(dbDirectoryName + "/" + UNCOMMON_TERMS_DB_NAME + "." + shardNumber);
		FileUtils.deleteQuietly(uncommonTermsDBfile);
		File allCommonTermsDBfile = new File(dbDirectoryName + "/" + COMMON_TERMS_DB_NAME + "." + shardNumber);
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

		this.commonTermsDB = DBMaker.fileDB(allCommonTermsDBfile).
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
		this.commonTermsMap =
				this.commonTermsDB.hashMapCreate(COMMON_TERMS_MAP_NAME).
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
