package com.shn.sid.search;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;

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
		
		ViolationsDetector detector = new ViolationsDetector(config, dv.indexName);
		detector.loadIndex();
		List<Violation> violations = detector.findViolations(tokens, dv.colThreshold, dv.cviolationsThreshold);
		detector.unloadIndex();
		
		for (Violation violation : violations) {
			LOG.info("Found violation: " + violation);
		}
	}
}
