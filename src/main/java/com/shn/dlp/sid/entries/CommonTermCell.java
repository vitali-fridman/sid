package com.shn.dlp.sid.entries;

public final class CommonTermCell extends Cell {
	
	public CommonTermCell(final Cell cell) {
        this(cell.row, cell.col, cell.getTerm());
    }
    
    public CommonTermCell(final int row, final int col, final byte[] term) {
        super(row, col, term);
    }
    
    @Override
    public int compareTo(final IndexEntry o) {
        return this.row - ((CommonTermCell)o).row;
    }
}
