package com.shn.dlp.sid.research;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.shn.dlp.sid.tools.ZipfTestDataFileGenerator;

public class CryptoFileReaderMultiThreaded {
	
	
	
	@Option(name="-f",usage="File Name to read", required=true)
	private String fileName;
	@Option(name="-d",usage="Database directory name", required=true)
	private String dbDirectoryName;
	@Option (name="-t",usage="Number of threads", required=true)
	private int numThreads;
	@Option (name="-s",usage="Number of shards", required=true)
	private int numberOfShards;
	
	public static void main(String[] args) throws IOException {
		
		CryptoFileReaderMultiThreaded cfr = new CryptoFileReaderMultiThreaded();
		CmdLineParser parser = new CmdLineParser(cfr);
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			return;
		}
		
		long start=System.nanoTime();
		
		FileUtils.deleteQuietly(new File(cfr.dbDirectoryName));
		FileUtils.forceMkdir(new File(cfr.dbDirectoryName));
		
		ExecutorService executor = Executors.newFixedThreadPool(cfr.numThreads);
		for (int i = 0; i < cfr.numberOfShards; i++) {
			Runnable worker = new CryptoFileReaderWorker(cfr.fileName, cfr.dbDirectoryName, cfr.numberOfShards, i);
		    executor.execute(worker);
		}
		executor.shutdown();
		while (!executor.isTerminated()) {			
		}
		
		long end = System.nanoTime();
		System.out.println("Time: " + (end - start)/1000000000d + " sec");
	}
}
