package com.shn.dlp.sid.lexer;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;

import org.antlr.v4.runtime.ANTLRFileStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.LexerInterpreter;
import org.antlr.v4.runtime.ParserInterpreter;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.tool.Grammar;
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
	    LexerInterpreter lexEngine = g.createLexerInterpreter(new ANTLRFileStream(testFile));
	    CommonTokenStream tokens = new CommonTokenStream(lexEngine);
	    
	    Vocabulary vc = g.getVocabulary();
	    
	    tokens.consume();
	    int i = tokens.index();
	    Token tk = tokens.get(i-1);
	    int type = tk.getType();
	    String name = vc.getSymbolicName(type);
	    String text = tk.getText();
	    System.out.println(tk);
	    
//	    tokens.fill();
//	    List<Token> allTokens = tokens.getTokens();
//	    for (Token token : allTokens) {
//	    	System.out.println(token);
//	    }
	}

}
