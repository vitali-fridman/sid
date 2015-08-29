package com.shn.dlp.sid.research;

import java.util.List;

import org.antlr.v4.runtime.*;


public class Antlr4Test {

	public static void main( String[] args) throws Exception 
	  {
		ANTLRFileStream input  = new ANTLRFileStream(args[0]);
		TestDefinitionLexer lexer = new TestDefinitionLexer(input);
		while(true) {
			Token token = lexer.nextToken();
			System.out.println(token);
			if (lexer._hitEOF) {
				break;
			}
		}
	    
	  }

}
