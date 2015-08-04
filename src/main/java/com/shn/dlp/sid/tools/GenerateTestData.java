package com.shn.dlp.sid.tools;

import java.io.IOException;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.ExampleMode;
import org.kohsuke.args4j.Option;

import com.shn.dlp.sid.security.CryptoException;

public class GenerateTestData {
	
	@Option(name="-f",usage="File Name to create", required=true)
	private String fileName;
	@Option (name="-c",usage="Number of columns", required=true)
	private int numColumns;
	@Option (name="-r",usage="Number of rows", required=true)
	private int numRows;
	@Option(name="-zc",usage="Number of Zipf columns", required=true)
	private int numZipfColumns;
	@Option(name="-zs",usage="Number of Zipf samles", required=true)
	private int numZipfSamples;
	@Option(name="-ze",usage="Zipf exponent", required=true)
	private double zipfExponent;
	@Option(name="-print",usage="Use if you need file content printed in clear")
	private boolean writeClearFile;
	
	public static void main(String[] args) {
		
		GenerateTestData gtd = new GenerateTestData();
		CmdLineParser parser = new CmdLineParser(gtd);
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			return;
		}

		TestDataFileGenerator generator = new TestDataFileGenerator(gtd.fileName, gtd.numColumns, 
				gtd.numRows, gtd.numZipfColumns, gtd.numZipfSamples, gtd.zipfExponent, gtd.writeClearFile);
		try {
			generator.generateFile();
		} catch (IOException | CryptoException e) {
			System.err.println("Error generating file: " + e.getMessage());
			return;
		}
		
		System.out.println("Data file generated.");
	}

}
