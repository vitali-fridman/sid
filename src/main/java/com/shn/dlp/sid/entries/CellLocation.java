package com.shn.dlp.sid.entries;

import java.io.Serializable;

import com.shn.dlp.sid.util.PrimeFinder;

public class CellLocation implements Serializable, Comparable<CellLocation>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6464996841957398408L;
	private final int row;
	private final byte column;
	
	public static final CellLocation EMPTY_CELL_LOCATION = new CellLocation(-1, 32);
	
	public CellLocation (int row, int column) {
		this.row = row;
		this.column = (byte) column;
	}

	@Override
	public int compareTo(CellLocation o) {
		if (this.row == o.getRow()) {
			return (this.column - o.getColumn());
		} else {
			return (this.row - o.getRow());
		}
	}
	
	public int getRow() {
		return this.row;
	}
	
	public byte getColumn() {
		return this.column;
	}
	
	@Override
	public int hashCode() {
		int rowPrime = PrimeFinder.nextPrime(this.row);
		int colPrime = PrimeFinder.nextPrime(this.column);
		return (this.row % rowPrime + this.column % colPrime);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CellLocation other = (CellLocation) obj;
		if (column != other.column)
			return false;
		if (row != other.row)
			return false;
		return true;
	}
}
