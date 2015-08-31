package com.shn.dlp.sid.indexer;

public class IndexDescriptor {
	private int headerLength;
	private int formatVersion;
	private String algorithm;
	private int termLength;
	private int numRows;
	private int numColumns;
	private int numShards;
	
	public IndexDescriptor(int headerLength, int formatVersion, String algorithm, 
			int termLength, int numRows, int numColumns, int numShards) {
		super();
		this.headerLength = headerLength;
		this.formatVersion = formatVersion;
		this.algorithm = algorithm;
		this.termLength = termLength;
		this.numRows = numRows;
		this.numColumns = numColumns;
		this.numShards = numShards;	
	}
	
	public int getHeaderLength() {
		return headerLength;
	}
	public void setHeaderLength(int headerLength) {
		this.headerLength = headerLength;
	}
	public int getFormatVersion() {
		return formatVersion;
	}
	public void setFormatVersion(int formatVersion) {
		this.formatVersion = formatVersion;
	}
	public String getAlgorithm() {
		return algorithm;
	}
	public void setAlgorithm(String algorithm) {
		this.algorithm = algorithm;
	}
	public int getTermLength() {
		return termLength;
	}
	public void setTermLength(int termLength) {
		this.termLength = termLength;
	}
	public int getNumRows() {
		return numRows;
	}
	public void setNumRows(int numRows) {
		this.numRows = numRows;
	}
	public int getNumColumns() {
		return numColumns;
	}
	public void setNumColumns(int numColumns) {
		this.numColumns = numColumns;
	}

	public int getNumShards() {
		return numShards;
	}

	public void setNumShards(int numShards) {
		this.numShards = numShards;
	}
	
}
