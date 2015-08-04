package com.shn.dlp.sid.entries;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class Cell extends IndexEntry {

	protected final int row;
    protected final int col;
    
    Cell(final int row, final int col, final byte[] term) {
        super(term);
        this.row = row;
        this.col = col;
    }
    
    public static Cell read(final DataInputStream in, final int termLength) throws IOException {
        final byte[] term = new byte[termLength];
        final int row = in.readInt();
        final byte col = in.readByte();
        in.readFully(term);
        return new Cell(row, col, term);
    }
    
    @Override
    public void write(final DataOutputStream out, final int termLength) throws IOException {
        out.writeInt(this.row);
        out.write(this.col);
        out.write(this.getTerm(), 0, termLength);
    }
    
    public int getRow() {
    	return this.row;
    }
    
    public int getColumn() {
    	return this.col;
    }

}
