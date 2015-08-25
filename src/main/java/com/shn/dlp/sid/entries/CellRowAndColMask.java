package com.shn.dlp.sid.entries;

import java.io.Serializable;

public class CellRowAndColMask implements Serializable, Comparable<CellRowAndColMask> {

	private static final long serialVersionUID = -4458148535179714302L;
	private final int row;
	private int colMask;
	
	public static final CellRowAndColMask EMPTY = new CellRowAndColMask(-1,0);
	
	public CellRowAndColMask (int row, int column) {
		this.row = row;
		this.colMask = 1 << column;
	}
	
	public static CellRowAndColMask create(int row, int columnMask) {
		CellRowAndColMask entry = new CellRowAndColMask(row, 0);
		entry.setColMask(columnMask);
		return entry;
	}
	
	public int getRow() {
		return this.row;
	}
	
	public int getMask() {
		return this.colMask;
	}
	
	private void setColMask( int colMask) {
		this.colMask = colMask;
	}
	
	public void addToMask(int column) {
		this.colMask = this.colMask | (1 << column);
	}

	@Override
	public int compareTo(CellRowAndColMask o) {
		return this.row - o.getRow();
	}

}
