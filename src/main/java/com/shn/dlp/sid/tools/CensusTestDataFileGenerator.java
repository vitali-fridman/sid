package com.shn.dlp.sid.tools;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import java.util.Scanner;

import com.google.common.primitives.Ints;
import com.shn.dlp.sid.security.CryptoException;
import com.shn.dlp.sid.security.Sha256Hmac;

public class CensusTestDataFileGenerator {

	public final static String CLEAR_SUFFIX = ".clear";
	public final static String CRYPTO_SUFFIX = ".crypto";
	private final static int FORMAT_VERSION = 1;
	private final static int BUFFER_SIZE = 1024*1024*100;
	
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
	private String[] lastNamesSampling;
	private String[] firstNamesSampling;
	private CensusEntry[] lastNames;
	private CensusEntry[] femaleFirstNames;
	private CensusEntry[] maleFirstNames;
	
	public CensusTestDataFileGenerator(String fileName, int numColumns, int numRows, boolean writeClearFile) {
		this.fileName = fileName;
		this.numColumns = numColumns;
		this.numRows = numRows;
		this.writeClearFile = writeClearFile;
		this.lastNamesSampling = new String[numRows];
		this.firstNamesSampling = new String[numRows];
	}

	public void generateFile() throws IOException, CryptoException {
		this.hmac = new Sha256Hmac();
		loadCensusData();
		fillAllSamplingData();
		
		this.cryptoWriter = new BufferedOutputStream(new FileOutputStream(new File(this.fileName + CRYPTO_SUFFIX)), BUFFER_SIZE);
		if (writeClearFile) {
			clearWriter = new BufferedWriter(new FileWriter(new File(this.fileName + CLEAR_SUFFIX)));
		} else {
			clearWriter = null;
		}
		writeHeader();
		writeData();
		close();
	}

	private void fillAllSamplingData() {
		fillSamplingDataforSet(this.lastNames, this.lastNamesSampling, 0, this.numRows, false);
		fillSamplingDataforSet(this.femaleFirstNames, this.firstNamesSampling, 0, this.numRows/2, true);
		fillSamplingDataforSet(this.maleFirstNames, this.firstNamesSampling, this.numRows/2 + 1, this.numRows, true);
	}

	private void fillSamplingDataforSet(CensusEntry[] censusEntries, String[] samplingSet, int rangeBottom, int rangeTop, boolean reverse) {
		int previousIndex = 0;
		
		if (!reverse) {
			for (int entryIndex=0; entryIndex<censusEntries.length; entryIndex++) {
				CensusEntry censusEntry = censusEntries[entryIndex];
				int topIndex = (int)Math.ceil((rangeTop - rangeBottom) * censusEntry.cummulativeProbability);
				if (topIndex == previousIndex) {
					topIndex++;
				}
				if (previousIndex == rangeTop) {
					break;
				}	
				for (int i=previousIndex; i<topIndex; i++) {
					samplingSet[i] = censusEntry.name;
				}
				previousIndex = topIndex;
			}
		} else {
			for (int entryIndex=censusEntries.length-1; entryIndex>=0; entryIndex--) {
				CensusEntry censusEntry = censusEntries[entryIndex];
				int topIndex = (int)Math.ceil((rangeTop - rangeBottom) * censusEntry.cummulativeProbability);
				if (topIndex == previousIndex) {
					topIndex++;
				}
				if (previousIndex == rangeTop) {
					break;
				}	
				for (int i=previousIndex; i<topIndex; i++) {
					samplingSet[i] = censusEntry.name;
				}
				previousIndex = topIndex;
			}
		}
		
		Random random = new Random();
		for (int i=previousIndex; i<rangeTop; i++) {
			samplingSet[i] = censusEntries[random.nextInt(censusEntries.length)].name;
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
			scanner.nextFloat();
			float cumulative = scanner.nextFloat()/100f;
			// skip rank
			scanner.nextLine();
			entries[i] = new CensusEntry(name, cumulative);
			i++;
		}
		scanner.close();
		is.close();
		
		return entries;
	}
	
private void writeData() throws CryptoException, IOException {
		
		// initialize required number if Zipf distributions
		Random[] randoms = new Random[this.numColumns-2];
		for (int i=0; i<this.numColumns-2; i++) {
			randoms[i] = new Random();
			randoms[i].setSeed(i);
		}
		
		int globalCounter = 0;
		// write data
		for (int row=0; row<this.numRows; row++) {
			for (int col=0; col<this.numColumns; col++) {
				String clearValue;
				byte[] cryptoValue;			
				if (col == 0) {
					clearValue = this.lastNamesSampling[row];
				} else if (col == 1){
					clearValue = this.firstNamesSampling[row];
				} else {
					clearValue = new Integer(globalCounter).toString();
					// clearValue = new Integer(randoms[col-2].nextInt(this.numRows)).toString();
					globalCounter++;
				}
				
				cryptoValue = this.hmac.computeDigest(clearValue);
				this.cryptoWriter.write(intToBytes(row));
				this.cryptoWriter.write(col);
				this.cryptoWriter.write(cryptoValue);
				
				if (this.clearWriter != null) {
					this.clearWriter.write(clearValue + "\t");
				}
			}
			if (this.clearWriter != null) {
				this.clearWriter.newLine();
			}
			if (row%10000 == 0) {
				System.out.println("Row: " + row/1000 +"K");
			}
		}
		
	}

	private void writeHeader() throws IOException {
		this.cryptoWriter.write(FORMAT_VERSION);
		this.cryptoWriter.write(intToBytes(this.numRows));
		this.cryptoWriter.write(this.numColumns);
		
		if (this.clearWriter != null) {
			this.clearWriter.write("Format Version: " + FORMAT_VERSION);
			this.clearWriter.newLine();
			this.clearWriter.write("Columns: " + this.numColumns);
			this.clearWriter.newLine();
			this.clearWriter.write("Rows: " + this.numRows);
			this.clearWriter.newLine();
			this.clearWriter.write("DATA:");
			this.clearWriter.newLine();
		}
	}
	
	private void close() throws IOException {
		this.cryptoWriter.close();
		if (clearWriter != null) {
			this.clearWriter.close();
		}
	}
	
	private static byte[] intToBytes(int value) {
		return Ints.toByteArray(value);
	}

	private static class CensusEntry {
		private String name;
		private float cummulativeProbability;
		
		 CensusEntry(String name, float cummulativeProbability) {
			this.name = name;
			this.cummulativeProbability = cummulativeProbability;
		}
		
	}
}
