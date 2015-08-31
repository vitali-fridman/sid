package com.shn.dlp.sid.lexer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

import org.antlr.v4.runtime.LexerInterpreter;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.UnbufferedCharStream;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.misc.Interval;
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
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
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
		FileInputStream fileStream = new FileInputStream(new File(testFile));
		UnbufferedCharStream charStream = new UnbufferedCharStream(fileStream, 100000000);
	    LexerInterpreter lexEngine = g.createLexerInterpreter(charStream);
	    
	    Vocabulary vc = g.getVocabulary();
	    Map<String, Integer> mapOfTypes = lexEngine.getTokenTypeMap();
	    Map<String, TokenProcessor> map = new HashMap<String, TokenProcessor>();
	    for (String rule : mapOfTypes.keySet()) {
	    	TokenProcessor processor;
	    	Class<?> c;
	    	if (rule == "EOF") {
	    		c = Class.forName("com.shn.dlp.sid.lexer.EOFprocessor");
	    	} else {
	    		String[] split = rule.split("_");
	    		c = Class.forName("com.shn.dlp.sid.lexer." + split[1] + "processor");
	    	}
	    	processor = (TokenProcessor) c.newInstance();
	    	map.put(rule, processor);
	    }
	    
	    long start=System.nanoTime();
	    
	    int i=0;
	    long accumulatedFileSize = 0;
	    String tokenProcessor = null;
	    while(true) {
	    	int mark = charStream.mark();
	    	Token token = lexEngine.nextToken();
	    	String tokenType = vc.getDisplayName(token.getType());
	    	if (token.getType() == Token.EOF) {
	    		break;
	    	}
	    	
	    	Interval interval = new Interval(token.getStartIndex(), token.getStopIndex());
	    	String text = charStream.getText(interval);
	    	charStream.release(mark);
	    	
	    	TokenProcessor processor = map.get(tokenType);
	    	processor.process(token, text, vc.getSymbolicName(token.getType()));
	    	
	    	if (Integer.MAX_VALUE - token.getStopIndex() < 1000 ) {
	    		accumulatedFileSize += token.getStopIndex();
	    		LOG.info("Reached maxumum chunk length, rolling");
	    		fileStream.close();
	    		fileStream = new FileInputStream(new File(testFile));
	    		fileStream.skip(accumulatedFileSize);
	    		charStream = new UnbufferedCharStream(fileStream, 100000000);
	    		lexEngine = g.createLexerInterpreter(charStream);
	    	}

	    	if (i%1000000 == 0) {
	    		LOG.info("On token " + String.format("%,d", i) + "processor: " + tokenProcessor);
	    	}
	    	i++;
	    }
	    
	    long end = System.nanoTime();
	    LOG.info("Found " + i + " tokens in " + (end - start)/1000000000d + " sec");
	   
	    
	    LogManager.shutdown();
	}

}
