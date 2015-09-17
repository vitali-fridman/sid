package com.shn.dlp.sid.indexer;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.lang3.StringUtils;

import com.google.common.primitives.Ints;

public class CryptoFileHeader {
	
	private final int formatVersion;
	private final String alrorithmName;
	private final int algorithmNameLength;
	private final int cryptoTermLength;
	private final int numRows;
	private final int numColumns;
	
	
	public CryptoFileHeader(int formatVersion, String alrorithmName, 
			int algorithmNameLength, int cryptoTermLength,
			int numRows, int numColumns) {
		this.formatVersion = formatVersion;
		this.alrorithmName = alrorithmName;
		this.algorithmNameLength = algorithmNameLength;
		this.cryptoTermLength = cryptoTermLength;
		this.numRows = numRows;
		this.numColumns = numColumns;
	}


	public int getFormatVersion() {
		return formatVersion;
	}


	public String getAlrorithmName() {
		return alrorithmName;
	}


	public int getAlgorithmNameLength() {
		return algorithmNameLength;
	}


	public int getCryptoTermLength() {
		return cryptoTermLength;
	}


	public int getNumRows() {
		return numRows;
	}


	public int getNumColumns() {
		return numColumns;
	}
	
	public void write(OutputStream os) throws IOException {
		os.write(this.formatVersion);
		os.write(StringUtils.rightPad(this.alrorithmName, this.algorithmNameLength).getBytes());
		os.write(this.cryptoTermLength);
		os.write(Ints.toByteArray(this.numRows));
		os.write(this.numColumns);
	}
	
	public static CryptoFileHeader read(DataInputStream is, int algNameLength) throws IOException {
		int formatVersion = is.read();
		byte[] algBytes = new byte[algNameLength];
		is.readFully(algBytes);
		String algName = new String(algBytes).trim();
		int termLength = is.read();
		int numRows = is.readInt();
		int numColumns = is.read();
		return new CryptoFileHeader(formatVersion, algName, algNameLength, termLength, numRows, numColumns);
	}
	
	public static void skip(DataInputStream is, int algNameLength) throws IOException {
		read(is, algNameLength);
	}
}
