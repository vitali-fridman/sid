package com.shn.dlp.sid.tools;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Scanner;

import com.shn.dlp.sid.security.Sha256Hmac;

public class CensusTestDataFileGenerator {

	public final static String CLEAR_SUFFIX = ".clear";
	public final static String CRYPTO_SUFFIX = ".crypto";
	private final static int FORMAT_VERSION = 1;
	private final static int BUFFER_SIZE = 1024*1024*100;
	private String[] lastNamesSampling;
	private String[] femaleFistNamesSampling;
	private String[] maleFirstNamesSampling;
	private CensusEntry[] lastNames;
	private CensusEntry[] femaleFirstNames;
	private CensusEntry[] maleFirstNames;
	private static final int NUM_LAST_NAMES = 88799;
	private static final int NUM_FEMALE_FIRST_NAMES = 4275;
	private static final int NUM_MALE_FIST_NAMES = 1219;
	private static final String LAST_NAMES_FILE = "dist.all.last";	
	private static final String FEMALE_FIRST_NAMES_FILE = "dist.female.first";
	private static final String MALE_FIRST_NAMES_FILE = "dist.male.first";


	private final String fileName;
	private final int numColumns;
	private final int numRows;
	private final boolean writeClearFile;
	private OutputStream cryptoWriter;
	private BufferedWriter clearWriter;
	private  Sha256Hmac hmac;
	
	public CensusTestDataFileGenerator(String fileName, int numColumns, int numRows, boolean writeClearFile) {
		this.fileName = fileName;
		this.numColumns = numColumns;
		this.numRows = numRows;
		this.writeClearFile = writeClearFile;
		this.lastNamesSampling = new String[numRows];
		this.femaleFistNamesSampling = new String[numRows];
		this.maleFirstNamesSampling = new String[numRows];
	}

	public void generateFile() throws IOException {
		this.hmac = new Sha256Hmac();
		loadCensusData();
		fillAllSamplingData();
	}

	private void fillAllSamplingData() {
		fillSamplingDataforSet(this.lastNames, this.lastNamesSampling);
		fillSamplingDataforSet(this.femaleFirstNames, this.femaleFistNamesSampling);
		fillSamplingDataforSet(this.maleFirstNames, this.maleFirstNamesSampling);
	}

	private void fillSamplingDataforSet(CensusEntry[] censusEntries, String[] samlingSet) {
		int previosIndex = 0;
		for (CensusEntry censusEntry : censusEntries) {
			int topIndex = numRows * censusEntry.cummulativeProbability;
		}
		
	}

	private void loadCensusData() throws IOException {
		this.lastNames = loadNames(LAST_NAMES_FILE, NUM_LAST_NAMES);		
		this.femaleFirstNames = loadNames(FEMALE_FIRST_NAMES_FILE, NUM_FEMALE_FIRST_NAMES);
		this.maleFirstNames = loadNames(MALE_FIRST_NAMES_FILE, NUM_MALE_FIST_NAMES);
	}
	
	private CensusEntry[] loadNames(String fileName, int numEntries) throws IOException {
		CensusEntry[] entries = new CensusEntry[numEntries];
		ClassLoader classLoader = this.getClass().getClassLoader();
		InputStream is = classLoader.getResourceAsStream(fileName);
		Scanner scanner = new Scanner(is);
		int i=0;
		while (scanner.hasNext()) {
			String name = scanner.next();
			// skip entry probability, we only need cumulative
			scanner.nextDouble();
			double cumulative = scanner.nextDouble()/100d;
			// skip rank
			scanner.nextLine();
			entries[i] = new CensusEntry(name, cumulative);
			i++;
		}
		scanner.close();
		is.close();
		
		return entries;
	}

	private static class CensusEntry {
		private String name;
		private double cummulativeProbability;
		
		 CensusEntry(String name, double cummulativeProbability) {
			this.name = name;
			this.cummulativeProbability = cummulativeProbability;
		}
		
	}

}
