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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionHandlerFilter;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shn.dlp.sid.entries.TermAndRow;
import com.shn.dlp.sid.entries.TermAndRowSerializer;
import com.shn.dlp.sid.security.Crypter;
import com.shn.dlp.sid.security.CryptoException;
import com.shn.dlp.sid.util.PeriodicGarbageCollector;
import com.shn.dlp.sid.util.SidConfiguration;
import static com.shn.dlp.sid.indexer.IndexComponents.COMMON_TERMS_AND_ROW_DB_NAME;
import static com.shn.dlp.sid.indexer.IndexComponents.COMMON_TERMS_AND_ROW_MAP_NAME;
import static com.shn.dlp.sid.indexer.IndexComponents.DESCRIPTOR_FILE_NAME;

public class Indexer {

	@Option(name="-f",usage="File Name to read", required=true)
	private String fileName;
	@Option(name="-p", usage="Properties File", required=false)
	private String propertiesFile;

	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());  

	private static DB[] commonTermsAndRowDBs;
	private static HTreeMap<TermAndRow, Integer>[] commonTermsAndRowMaps;
	private static int headerLength;
	private static int formatVersion;
	private static String algorithm;
	private static int fullTermLength;
	private static int retainedTermLength;
	private static int numRows;
	private static int numColumns;
	private static int numShards;


	public static void main(String[] args) throws IOException, CryptoException {
		Indexer cfr = new Indexer();
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

		numShards = readCryptoFileHeader(config, fileToIndex);
		
		if (numShards < 1) {
			LOG.error("Error Calculation number of shards");
			System.exit(-1);
			LogManager.shutdown();
		}
		
		createCommonTermsDBandMaps(config);

		
		ExecutorService executor= Executors.newFixedThreadPool(config.getIndexerNumThreads());
		List<Future<Boolean>> results = new ArrayList<>();
		boolean[] completed = new boolean[numShards];

		for (int i = 0; i < numShards; i++) {
			Callable<Boolean> worker = new IndexerWorker(config, fileToIndex, numRows, numShards, commonTermsAndRowMaps, 
					fullTermLength, retainedTermLength, i);
			Future<Boolean> result = executor.submit(worker);
			results.add(result);
		}

		boolean fail = false;
		int remaning  = numShards;
		while(!fail && remaning > 0) {
			for (int i=0; i<numShards; i++) {
				Future<Boolean> future = results.get(i);
				try {
					if (!completed[i] && future. isDone()) {
						if (!future.get()) {
							fail = true;
							LOG.error("Indexer workers for shard# " + i + " failed");
							break;
						} else {
							completed[i] = true;
							remaning--;
						}
					}
				} catch (InterruptedException | ExecutionException e) {
					fail = true;
					LOG.error("Indexer workers for shard# " + i + " threw exception", e); 
					break;
				}
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {};
		}

		if (!fail) {
			executor.shutdown();
		} else {
			executor.shutdownNow();
		}

		try {
			executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			LOG.error("Indexer was interrupted before normal completion, exiting", e);
			LogManager.shutdown();
			System.exit(-1);
		}
		
		if (fail) {
			LOG.error("One of the indexer workers failed, Exiting");
			LogManager.shutdown();
			System.exit(-1);
		}

		commitCommonTermsAndRowDBs();
		
		writeDescriptorFile(tempDirectory);
		
		IndexFilterCreator filterCreator = new IndexFilterCreator(config, commonTermsAndRowMaps);
		filterCreator.createTermAndRowFilters();
		
		closeCommonTermsAndRowDBs();
		
		String indexDirectory = config.getIndexesDirectory() + File.separator + cfr.fileName;
		FileUtils.deleteQuietly(new File(indexDirectory));
		FileUtils.moveDirectory(new File(tempDirectory), new File(indexDirectory));
		


		long end = System.nanoTime();
		if (!fail) {
			LOG.info("INdexing of  " + cfr.fileName + " took " + (end - start)/1000000000d + " sec");
		} else {
			LOG.info("Indexig of  " + cfr.fileName + " failed after " + (end - start)/1000000000d + " sec");
		}
		
		LogManager.shutdown();
	}

	private static void writeDescriptorFile(String indexDirectory) 
			throws JsonGenerationException, JsonMappingException, IOException {
		IndexDescriptor descriptor = new IndexDescriptor(headerLength, formatVersion, algorithm, fullTermLength,
				retainedTermLength,  numRows, numColumns, numShards);
		ObjectMapper mapper = new ObjectMapper();
		mapper.writeValue(new File(indexDirectory + File.separator + DESCRIPTOR_FILE_NAME), descriptor);
	}

	private static void commitCommonTermsAndRowDBs() {
		for (int i=0; i< numShards; i++) {
			commonTermsAndRowDBs[i].commit();
		}
	}

	private static void closeCommonTermsAndRowDBs() {
		for (int i=0; i< numShards; i++) {
			commonTermsAndRowDBs[i].close();
		}
	}
	
	private static void createCommonTermsDBandMaps(SidConfiguration config) throws CryptoException {

		commonTermsAndRowDBs = new DB[numShards];
		commonTermsAndRowMaps = new HTreeMap[numShards];
		
		for (int i=0; i<numShards; i++) {
			File commonTermsDBfile = new File(config.getIndexerTempDirectory() + "/" + COMMON_TERMS_AND_ROW_DB_NAME + "." + i);
			commonTermsAndRowDBs[i] = DBMaker.fileDB(commonTermsDBfile).
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
		
			commonTermsAndRowMaps[i] =
					commonTermsAndRowDBs[i].hashMapCreate(COMMON_TERMS_AND_ROW_MAP_NAME).
					valueSerializer(Serializer.INTEGER).
					keySerializer(new TermAndRowSerializer(retainedTermLength)).
					counterEnable().					
					make();
		}
	
	}


	private static int readCryptoFileHeader(SidConfiguration config, String cryptoFileName) throws IOException {
		DataInputStream dis = null;
		try {
			dis = new DataInputStream(new BufferedInputStream(new FileInputStream(cryptoFileName)));
			headerLength = dis.readByte(); // ignore
			formatVersion = dis.readByte();
			if (formatVersion != config.getCryptoFileFormatVersion()) {
				LOG.error("Wrong crypto file format version");
				LogManager.shutdown();
				return -1;
			}
			byte[] alg = new byte[config.getCryptoFileHeaderAlgoritmNameLength()];
			dis.readFully(alg);
			algorithm = new String(alg).trim();
			fullTermLength = dis.readByte(); // ignore
			numRows = dis.readInt();
			retainedTermLength = config.getTemSizeToRetain(numRows);
			numColumns = dis.readByte();
			if (numColumns < 1 || numColumns > 30) {
				LOG.error("Number of columns is " + numColumns + " but it must be between 1 and 30");
				LogManager.shutdown();
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
			LogManager.shutdown();
			return -1;
		} catch (IOException e) {
			LOG.error("Error reading header of Crypto File: " + cryptoFileName + e.getMessage());
			LogManager.shutdown();
			return -1;
		} finally {
			if (dis != null ) dis.close();
		}
	}
}
