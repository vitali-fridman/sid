package com.shn.dlp.sid.indexer;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.ExampleMode;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionHandlerFilter;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shn.dlp.sid.entries.CellRowAndColMaskListSerializer;
import com.shn.dlp.sid.entries.RawTerm;
import com.shn.dlp.sid.entries.RawTermSerializer;
import com.shn.dlp.sid.entries.TermAndRow;
import com.shn.dlp.sid.entries.TermAndRowSerializer;
import com.shn.dlp.sid.security.Crypter;
import com.shn.dlp.sid.security.CryptoException;
import com.shn.dlp.sid.util.PeriodicGarbageCollector;
import com.shn.dlp.sid.util.SidConfiguration;

public class CryptoFileIndexer {

	@Option(name="-f",usage="File Name to read", required=true)
	private String fileName;
	@Option(name="-p", usage="Properties File", required=false)
	private String propertiesFile;

	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());  

	private final static String COMMON_TERMS_MAP_NAME = "CommonTermsMap";
	private final static String COMMON_TERMS_DB_NAME = "ConmmonTermsDB";
	private static DB commonTermsDB;
	private static HTreeMap<TermAndRow, Integer> commonTermsMap;


	public static void main(String[] args) throws IOException, CryptoException {
		CryptoFileIndexer cfr = new CryptoFileIndexer();
		CmdLineParser parser = new CmdLineParser(cfr);
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
			LOG.error(e.getMessage());
			LOG.error(parser.printExample(OptionHandlerFilter.ALL));
			return;
		}

		long start=System.nanoTime();

		SidConfiguration config;
		if (cfr.propertiesFile != null) {
			config = new SidConfiguration(cfr.propertiesFile);
		} else {
			config = new SidConfiguration();
		}

		String fileToIndex = config.getDataFilesDrictory() + File.separator + cfr.fileName 
				+ Crypter.CRYPRO_FILE_SUFFIX;
		if (!Files.exists(Paths.get(fileToIndex))) {
			LOG.error("Crypto File does not exist");
			System.exit(-1);
		}

		if (!Files.exists(Paths.get(config.getIndexesDirectory()))) {
			LOG.error("Indexes directory does not exist");
			System.exit(-1);
		}

		String tempDirectory = config.getIndexerTempDirectory();
		FileUtils.deleteQuietly(new File(tempDirectory));
		FileUtils.forceMkdir(new File(tempDirectory));

		PeriodicGarbageCollector pgc = new PeriodicGarbageCollector(config.getIndexerGcPeriod());
		pgc.setDaemon(true);
		pgc.start();

		createCommonTermsDBandMap(config);

		int numShards = calculateNumberOfShards(config, fileToIndex);
		if (numShards < 1) {
			LOG.error("Error Calculation number of shards");
			System.exit(-1);
		}
		ExecutorService executor= Executors.newFixedThreadPool(config.getIndexerNumThreads());
		List<Future<Boolean>> results = new ArrayList<>();

		for (int i = 0; i < numShards; i++) {
			Callable<Boolean> worker = new CryptoFileIndexerWorker(config, fileToIndex, numShards, commonTermsMap, i);
			Future<Boolean> result = executor.submit(worker);
			results.add(result);
		}

		boolean fail = false;
		int remaning  = numShards;
		while(!fail && remaning > 0) {
			for (Future<Boolean> future : results) {
				try {
					if (future. isDone()) {
						if (!future.get()) {
							fail = true;
							LOG.error("One of the indexer workers failed");
							break;
						} else {
							remaning--;
						}
					}
				} catch (InterruptedException | ExecutionException e) {
					fail = true;
					LOG.error("One of the indexer workers threw exception", e); 
					break;
				}
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// do nothing
			}
		}

		if (!fail) {
			executor.shutdown();
		} else {
			executor.shutdownNow();
		}

		while (!executor.isTerminated()) {			
		}

		closeCommonTermsDB();

		long end = System.nanoTime();
		if (!fail) {
			LOG.info("INdexing of  " + cfr.fileName + " took " + (end - start)/1000000000d + " sec");
		} else {
			LOG.info("Indexig of  " + cfr.fileName + " failed after " + (end - start)/1000000000d + " sec");
		}
	}

	private static void closeCommonTermsDB() {
		commonTermsDB.commit();
		commonTermsDB.close();
	}

	private static void createCommonTermsDBandMap(SidConfiguration config) throws CryptoException {
		File commonTermsDBfile = new File(config.getIndexerTempDirectory() + "/" + COMMON_TERMS_DB_NAME);
		FileUtils.deleteQuietly(commonTermsDBfile);

		commonTermsDB = DBMaker.fileDB(commonTermsDBfile).
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
		
		commonTermsMap =
				commonTermsDB.hashMapCreate(COMMON_TERMS_MAP_NAME).
					valueSerializer(Serializer.INTEGER).
					keySerializer(new TermAndRowSerializer(config)).
					counterEnable().					
					make();	
		
	}


	private static int calculateNumberOfShards(SidConfiguration config, String cryptoFileName) throws IOException {
		DataInputStream dis = null;
		try {
			dis = new DataInputStream(new BufferedInputStream(new FileInputStream(cryptoFileName)));
			int headerLength = dis.readByte(); // ignore
			int formatVersion = dis.readByte();
			if (formatVersion != config.getCryptoFileFormatVersion()) {
				LOG.error("Wrong crypto file format version");
				return -1;
			}
			byte[] alg = new byte[config.getCryptoFileHeaderAlgoritmNameLength()];
			dis.readFully(alg);
			int termLength = dis.readByte(); // ignore
			int numRows = dis.readInt();
			int numColumns = dis.readByte();
			if (numColumns < 1 || numColumns > 30) {
				LOG.error("Number of columns is " + numColumns + " but it must be between 1 and 30");
				return -1;
			}
			long numbCells = (long)numColumns * (long)numRows;
			if ( numbCells > 2000000000) {
				LOG.error("Number of cells is " + numbCells + " but maximum allowed is 2 Billion");
				return -1;
			}
			int numCells = numColumns*numRows;
			int numShards = (int)Math.ceil(numCells / (double) config.getIndexerOptimalCellsPerShard());
			if (config.getIndexerNumThreads() > numShards) {
				return config.getIndexerNumThreads();
			} else {
				return numShards;
			}

		} catch (FileNotFoundException e) {
			LOG.error("Crypto File: " + cryptoFileName + " not found");
			return -1;
		} catch (IOException e) {
			LOG.error("Error reading header of Crypto File: " + cryptoFileName + e.getMessage());
			return -1;
		} finally {
			if (dis != null ) dis.close();
		}
	}
}
