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

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.ExampleMode;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionHandlerFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shn.dlp.sid.security.Sha256Hmac;
import com.shn.dlp.sid.util.PeriodicGarbageCollector;
import com.shn.dlp.sid.util.SidConfiguration;

public class CryptoFileIndexer {
		
	@Option(name="-f",usage="File Name to read", required=true)
	private String fileName;
	@Option(name="-p", usage="Properties File", required=false)
	private String propertiesFile;

	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());  

	
	public static void main(String[] args) throws IOException {
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
				+ Sha256Hmac.CRYPRO_FILE_SUFFIX;
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
		
		int numShards = calculateNumberOfShards(config, fileToIndex);
		if (numShards < 1) {
			LOG.error("Error Calculation number of shards");
			System.exit(-1);
		}
		ExecutorService executor = Executors.newFixedThreadPool(config.getIndexerNumThreads());
		List<Future<Boolean>> results = new ArrayList<>();
		
		for (int i = 0; i < numShards; i++) {
			Callable<Boolean> worker = new CryptoFileIndexerWorker(config, fileToIndex, numShards, i);
		    Future<Boolean> result = executor.submit(worker);
		    results.add(result);
		}
		
		boolean fail = false;
		for (Future<Boolean> future : results) {
			try {
				if (!future.get()) {
					fail = true;
					LOG.error("One of the indexer workers failed");
					break;
				}
			} catch (InterruptedException | ExecutionException e) {
				fail = true;
				LOG.error("One of the indexer workers threw exception", e);
				break;
			}
		}
		
		if (!fail) {
			executor.shutdown();
		} else {
			executor.shutdownNow();
		}
		
		while (!executor.isTerminated()) {			
		}
		
		long end = System.nanoTime();
		LOG.info("Time took to index " + cfr.fileName + (end - start)/1000000000d + " sec");
	}
	
	private static int calculateNumberOfShards(SidConfiguration config, String cryptoFileName) throws IOException {
		DataInputStream dis = null;
		try {
			dis = new DataInputStream(new BufferedInputStream(new FileInputStream(cryptoFileName)));
			int formatVersion = dis.readByte();
			if (formatVersion != config.getCryptoFileFormatVersion()) {
				LOG.error("Wrong crypto file format version");
				return -1;
			}
			int numColumns = dis.readInt();
			int numRows = dis.readByte();
			int numCells = numColumns*numRows;
			int numShards = (int)Math.ceil(numCells / (double) config.getIndexerOptimalCellsPerShard());
			return numShards;
			
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
