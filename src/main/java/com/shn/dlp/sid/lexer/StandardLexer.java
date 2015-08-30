package com.shn.dlp.sid.lexer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.List;

import org.antlr.v4.runtime.ANTLRFileStream;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.LexerInterpreter;
import org.antlr.v4.runtime.ParserInterpreter;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.UnbufferedCharStream;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.tool.Grammar;
import org.apache.log4j.LogManager;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionHandlerFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shn.dlp.sid.util.SidConfiguration;


public class StandardLexer {
	
	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());  

	@Option(name="-g",usage="Grammar file", required=true)
	private String grammarFile;
	@Option(name="-f",usage="Test file", required=true)
	private String testFile;
	@Option(name="-p", usage="Properties File", required=false)
	private String propertiesFile;
	
	public static void main(String[] args) throws IOException {
		StandardLexer sl = new StandardLexer();
		CmdLineParser clParser = new CmdLineParser(sl);
		try {
			clParser.parseArgument(args);
		} catch (CmdLineException e) {
			LOG.error(e.getMessage());
			LOG.error(clParser.printExample(OptionHandlerFilter.ALL));
			return;
		}
		
		SidConfiguration config = new SidConfiguration();
		
		String grammarFile = config.getGramarFileDirectory() + File.separator +  sl.grammarFile;
		String testFile = sl.testFile;

		Grammar g = Grammar.load(grammarFile);
		UnbufferedCharStream charStream = new UnbufferedCharStream(new FileInputStream(new File(testFile)), 100000000);
	    LexerInterpreter lexEngine = g.createLexerInterpreter(charStream);
	    // CommonTokenStream tokens = new CommonTokenStream(lexEngine);
	    
	    Vocabulary vc = g.getVocabulary();
	    
	    long start=System.nanoTime();
	    
	    int i=0;
	    while(true) {
	    	int mark = charStream.mark();
	    	Token token = lexEngine.nextToken();
	    	if (token.getType() == Token.EOF) {
	    		break;
	    	}
	    	Interval interval = new Interval(token.getStartIndex(), token.getStopIndex());
	    	String text = charStream.getText(interval);
	    	charStream.release(mark);

	    	if (i%100000 == 0) {
	    		LOG.info("On token " + i);
	    	}
	    	i++;
	    }
	    
	    long end = System.nanoTime();
	    LOG.info("Found " + i + " tokens in " + (end - start)/1000000000d + " sec");
	   
	    
	    LogManager.shutdown();
	}

}
