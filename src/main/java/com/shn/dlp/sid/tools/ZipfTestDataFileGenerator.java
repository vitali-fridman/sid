package com.shn.dlp.sid.tools;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;

import com.google.common.primitives.Ints;
import com.shn.dlp.sid.security.CryptoException;
import com.shn.dlp.sid.security.Sha256Hmac;

public class ZipfTestDataFileGenerator {

	public final static String CLEAR_SUFFIX = ".clear";
	public final static String CRYPTO_SUFFIX = ".crypto";
	private final static int FORMAT_VERSION = 1;
	private final static int BUFFER_SIZE = 1024*1024*100;
	
	private final String fileName;
	private final int numColumns;
	private final int numRows;
	private final int numZipfColumns;
	private final int numZipfSamples;
	private final boolean writeClearFile;
	private final double zipfExponent;
	private OutputStream cryptoWriter;
	private BufferedWriter clearWriter;
	private  Sha256Hmac hmac;
	
	ZipfTestDataFileGenerator(String fileName, int numColumns, int numRows, int numZipfColumns, int numZipfSamples, double zipfExponent, boolean writeClearFile) {
		this.fileName = fileName;
		this.numColumns = numColumns;
		this.numRows = numRows;
		this.numZipfColumns = numZipfColumns;
		this.numZipfSamples = numZipfSamples;
		this.writeClearFile = writeClearFile;
		this.zipfExponent = zipfExponent;
	}

	public void generateFile() throws IOException, CryptoException {
		this.hmac = new Sha256Hmac();
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
	
	private void writeData() throws CryptoException, IOException {
		
		// initialize required number if Zipf distributions
		RandomGenerator[] randoms = new RandomGenerator[this.numColumns];
		ZipfDistribution[] distributions = new ZipfDistribution[this.numZipfColumns];
		for (int i=0; i<this.numColumns; i++) {
			randoms[i] = new JDKRandomGenerator();
			randoms[i].setSeed(i);
			if (i<this.numZipfColumns) {
				// distributions[i] = new ZipfDistribution(randoms[i], this.numZipfSamples, this.zipfExponent);
				distributions[i] = new ZipfDistribution(this.numZipfSamples, this.zipfExponent);
			}
		}
		
		// write data
		for (int row=0; row<this.numRows; row++) {
			for (int col=0; col<this.numColumns; col++) {
				int clearValue;
				byte[] cryptoValue;			
				if (col < this.numZipfColumns) {
					clearValue = distributions[col].sample();
				} else {
					clearValue = randoms[col].nextInt();
				}
				cryptoValue = this.hmac.computeDigest(new Integer(clearValue).toString());
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
			this.clearWriter.write("Zipf columns: " + this.numZipfColumns);
			this.clearWriter.newLine();
			this.clearWriter.write("Zipf samples: " + this.numZipfSamples);
			this.clearWriter.newLine();
			this.clearWriter.write("Zipf exponent: " + this.zipfExponent);
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
}
