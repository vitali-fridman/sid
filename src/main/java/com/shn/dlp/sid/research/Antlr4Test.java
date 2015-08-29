package com.shn.dlp.sid.research;

import java.util.List;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.pattern.RuleTagToken;


public class Antlr4Test {

	public static void main( String[] args) throws Exception 
	  {
		ANTLRFileStream input  = new ANTLRFileStream(args[0]);
		TestDefinitionLexer lexer = new TestDefinitionLexer(input);
		
		Vocabulary vc = lexer.getVocabulary();
		
		while(!lexer._hitEOF) {
			Token token = lexer.nextToken();
			int type = token.getType();
			System.out.println("Display Name: " + vc.getDisplayName(type));
			System.out.println("Literal Name: " + vc.getLiteralName(type));
			System.out.println("Symbolic Name: " + vc.getSymbolicName(type));
			System.out.println("Text: " + token.getText());
			System.out.println(token);
		}
	    
	  }

}
