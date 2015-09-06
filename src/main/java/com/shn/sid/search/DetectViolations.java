package com.shn.sid.search;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;

import org.apache.log4j.LogManager;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionHandlerFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shn.dlp.sid.entries.RawTerm;
import com.shn.dlp.sid.entries.Token;
import com.shn.dlp.sid.lexer.TestLexer;
import com.shn.dlp.sid.security.CryptoException;
import com.shn.dlp.sid.util.SidConfiguration;

public class DetectViolations {

	@Option(name="-f",usage="File Name to read", required=true)
	private String fileName;
	@Option(name="-i",usage="Index Name", required=true)
	private String indexName;
	@Option(name="-c",usage="Column Threashold", required=true)
	private int colThreshold;
	@Option(name="-v",usage="Violations Threashold", required=true)
	private int cviolationsThreshold;
	
	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	
	public static void main(String[] args) throws IOException, CryptoException {
		DetectViolations dv = new DetectViolations();
		CmdLineParser parser = new CmdLineParser(dv);
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
			LOG.error(e.getMessage());
			LOG.error(parser.printExample(OptionHandlerFilter.ALL));
			return;
		}
		
		SidConfiguration config = new SidConfiguration();
		TestLexer lexer = new TestLexer(config);
		List<Token> tokens = lexer.scan(dv.fileName);
		
		int numIndexes = 5;
		ViolationsDetector[] detectors = new ViolationsDetector[numIndexes];
		for(int i=0; i<numIndexes; i++) {
			// detectors[i] = new ViolationsDetector(config, dv.indexName);
			detectors[i] = new ViolationsDetector(config, new Integer(i).toString());
			detectors[i].loadIndex();
		}
		
		long start = 0;
		long end = 0;
		List<Violation> violations = null;
		start = System.nanoTime();
		
		int iterations = 1000;
		for (int j=0; j<iterations; j++) {
			for (int i=0; i<numIndexes; i++) {
				violations = detectors[i].findViolations(tokens, dv.colThreshold, dv.cviolationsThreshold);
			}
		}
			
		

		for(int i=0; i<numIndexes; i++) {
			detectors[i].unloadIndex();
		}
		
		if (violations != null) {
			for (Violation violation : violations) {
				LOG.info("Found violation: " + violation.toString());
			}
		}
		
		end = System.nanoTime();
		LOG.info("Detection took " + (end-start)/1000000d/(double)(iterations*numIndexes) + " ms");
		
		LogManager.shutdown();
	}
}
