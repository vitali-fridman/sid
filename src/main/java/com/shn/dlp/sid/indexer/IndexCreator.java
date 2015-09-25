package com.shn.dlp.sid.indexer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionHandlerFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shn.dlp.sid.security.Crypter;
import com.shn.dlp.sid.util.SidConfiguration;

public class IndexCreator {

	@Option(name="-f",usage="File Name to read", required=true)
	private String fileName;
	@Option(name="-p", usage="Properties File", required=false)
	private String propertiesFile;

	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass()); 
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
		
		IndexCreator ic = new IndexCreator();
		CmdLineParser parser = new CmdLineParser(ic);
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
			LOG.error(e.getMessage());
			LOG.error(parser.printExample(OptionHandlerFilter.ALL));
			return;
		}

		long start=System.nanoTime();

		SidConfiguration config;
		if (ic.propertiesFile != null) {
			config = new SidConfiguration(ic.propertiesFile);
		} else {
			config = new SidConfiguration();
		}
		
		String fileToIndex = config.getDataFilesDrictory() + File.separator + ic.fileName 
				+ Crypter.CRYPRO_FILE_SUFFIX;
		if (!Files.exists(Paths.get(fileToIndex))) {
			LOG.error("Crypto File does not exist");
			return;
		}

		if (!Files.exists(Paths.get(config.getIndexesDirectory()))) {
			LOG.error("Indexes directory does not exist");
			return;
		}
		
		Indexer indexer = new Indexer(config, fileToIndex);
		
		boolean indexingResult = indexer.index();
		
		long end = System.nanoTime();
		if (indexingResult) {
			LOG.info("INdexing of  " + ic.fileName + " took " + (end - start)/1000000000d + " sec");
		} else {
			LOG.info("Indexig of  " + ic.fileName + " failed after " + (end - start)/1000000000d + " sec");
		}
	}
		
}
