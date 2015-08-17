package com.shn.dlp.sid.research;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

public class CryptoFileReaderMultiThreaded {
	
	private static final double OPTIMAL_CELLS_PER_SHARD = 15000000d;
	
	@Option(name="-f",usage="File Name to read", required=true)
	private String fileName;
	@Option(name="-d",usage="Database directory name", required=true)
	private String dbDirectoryName;
	@Option (name="-t",usage="Number of threads", required=true)
	private int numThreads;
	// @Option (name="-s",usage="Number of shards", required=true) 
	// private int numberOfShards;
	
	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());  

	
	public static void main(String[] args) throws IOException {
		
		CryptoFileReaderMultiThreaded cfr = new CryptoFileReaderMultiThreaded();
		CmdLineParser parser = new CmdLineParser(cfr);
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
			LOG.error(e.getMessage());
			LOG.error(parser.printExample(OptionHandlerFilter.ALL));
			return;
		}
		
		long start=System.nanoTime();
		
		FileUtils.deleteQuietly(new File(cfr.dbDirectoryName));
		FileUtils.forceMkdir(new File(cfr.dbDirectoryName));
		
		PeriodicGarbageCollector pgc = new PeriodicGarbageCollector(10000);
		pgc.setDaemon(true);
		pgc.start();
		
		int numShards = calculateNumberOfShards(cfr.fileName);
		ExecutorService executor = Executors.newFixedThreadPool(cfr.numThreads);
		for (int i = 0; i < numShards; i++) {
			Runnable worker = new CryptoFileReaderWorker(cfr.fileName, cfr.dbDirectoryName, numShards, i);
		    executor.execute(worker);
		}
		executor.shutdown();
		while (!executor.isTerminated()) {			
		}
		
		long end = System.nanoTime();
		LOG.info("Time: " + (end - start)/1000000000d + " sec");
	}
	
	private static int calculateNumberOfShards(String cryptoFileName) {
		try {
			DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(cryptoFileName + Sha256Hmac.CRYPRO_FILE_SUFFIX)));
			int formatVersion = dis.readByte();
			int numColumns = dis.readInt();
			int numRows = dis.readByte();
			int numCells = numColumns*numRows;
			int numShards = (int)Math.ceil(numCells / OPTIMAL_CELLS_PER_SHARD);
			return numShards;
			
		} catch (FileNotFoundException e) {
			LOG.error("Crypto File: " + cryptoFileName + " not found");
			return -1;
		} catch (IOException e) {
			LOG.error("Error reading header of Crypto File: " + cryptoFileName + e.getMessage());
			return -1;
		}
	}
}
