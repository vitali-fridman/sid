package com.shn.dlp.sid.tools;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.junit.Test;

public class GenerateCensusTestDataTest {
	private final static String SHORT_FILE_NAME = "500KCensus";
	private final static String FILE_NAME = "500KCensus.crypto"; 
	private final static String GOLD_FILE_NAME = "500KCensus.crypto.gold";
	
	
	@Test
	public void testMain() {
		String tempDirectory = System.getProperty("java.io.tmpdir");
		ClassLoader classLoader = this.getClass().getClassLoader();
		InputStream is = classLoader.getResourceAsStream(FILE_NAME);
		try {
			Files.copy(is, Paths.get(tempDirectory, GOLD_FILE_NAME), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			fail("Error creating Gold File in temporary directory.");
		}
		
		String[] args = new String[] {"-f", tempDirectory + "/" + SHORT_FILE_NAME, "-c", "5", "-r", "100000"};
		GenerateCensusTestData.main(args);
		
		File goldFile = new File(tempDirectory, GOLD_FILE_NAME);
		File generatedFile = new File(tempDirectory, FILE_NAME);
		try {
			assertTrue(com.google.common.io.Files.equal(goldFile, generatedFile));
		} catch (IOException e) {
			fail("Error comparing files. " + e.getMessage());
		}
		
	}
	
	@Test
	public void testMainWithClearPrint() {
		String tempDirectory = System.getProperty("java.io.tmpdir");
		ClassLoader classLoader = this.getClass().getClassLoader();
		InputStream is = classLoader.getResourceAsStream(FILE_NAME);
		try {
			Files.copy(is, Paths.get(tempDirectory, GOLD_FILE_NAME), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			fail("Error creating Gold File in temporary directory.");
		}
		
		String[] args = new String[] {"-f", tempDirectory + "/" + SHORT_FILE_NAME, "-c", "5", "-r", "100000", "-print"};
		GenerateCensusTestData.main(args);
		
		File goldFile = new File(tempDirectory, GOLD_FILE_NAME);
		File generatedFile = new File(tempDirectory, FILE_NAME);
		try {
			assertTrue(com.google.common.io.Files.equal(goldFile, generatedFile));
		} catch (IOException e) {
			fail("Error comparing files. " + e.getMessage());
		}
		
	}

}
