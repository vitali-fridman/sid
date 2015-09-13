package com.shn.dlp.sid.entries;

import java.io.DataInputStream;
import java.io.IOException;

import com.google.common.primitives.Ints;

public class Cell {

	protected final int row;
    protected final int col;
    private final byte[] value;
    
    Cell(final int row, final int col, final byte[] value) {
        this.row = row;
        this.col = col;
        this.value = value;
    }
    
    public static Cell read(final DataInputStream in, final int cryptoFileTermLength) throws IOException {
    	final byte[] term = new byte[cryptoFileTermLength];
        final byte[] rowBytes = new byte[4];
        in.readFully(rowBytes);
        final int rownumber = Ints.fromByteArray(rowBytes);
        final byte col = in.readByte();
        in.readFully(term);
        return new Cell(rownumber, col, term);
    }
    
    public int getRow() {
    	return this.row;
    }
    
    public int getColumn() {
    	return this.col;
    }

	public byte[] getValue() {
		return value;
	}
}
