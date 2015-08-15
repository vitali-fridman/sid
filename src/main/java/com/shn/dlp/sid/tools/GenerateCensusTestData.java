package com.shn.dlp.sid.tools;

import java.io.IOException;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.shn.dlp.sid.security.CryptoException;

public class GenerateCensusTestData {

	@Option(name="-f",usage="File Name to create", required=true)
	private String fileName;
	@Option (name="-c",usage="Number of columns", required=true)
	private int numColumns;
	@Option (name="-r",usage="Number of rows", required=true)
	private int numRows;
	@Option(name="-print",usage="Use if you need file content printed in clear")
	private boolean writeClearFile;
	
	public static void main(String[] args) {
		GenerateCensusTestData gtd = new GenerateCensusTestData();
		CmdLineParser parser = new CmdLineParser(gtd);
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			return;
		}
		
		if (gtd.numColumns < 3) {
			System.err.println("Number of columns must be at least 3.");
			return;
		}
		
		if (gtd.numRows < 100000) {
			System.err.println("Number of rows must be at least 100K.");
			return;
		}
		
		CensusTestDataFileGenerator generator = new CensusTestDataFileGenerator(gtd.fileName, gtd.numColumns, 
				gtd.numRows, gtd.writeClearFile);
		try {
			generator.generateFile();
		} catch (IOException | CryptoException e) {
			System.err.println("Error generating file: " + e.getMessage());
			return;
		}
		
		System.out.println("Data file generated.");

	}

}
