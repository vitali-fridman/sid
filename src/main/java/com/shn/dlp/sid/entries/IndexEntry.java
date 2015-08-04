package com.shn.dlp.sid.entries;

import java.io.DataOutputStream;
import java.io.IOException;

public abstract class IndexEntry implements Comparable<IndexEntry>{
	
	private final byte[] term;
    
    IndexEntry(final byte[] term) {
        this.term = term;
    }
    
    public abstract void write(final DataOutputStream os, final int legth) throws IOException;
    
    public void write(final DataOutputStream out) throws IOException {
        this.write(out, this.getTerm().length);
    }
    
    private int compareTo(final IndexEntry other, final int termLength) {
        final byte[] otherTerm = other.getTerm();
        for (int i = 0; i < termLength; ++i) {
            final int diff = this.getTerm()[i] - otherTerm[i];
            if (diff != 0) {
                return diff;
            }
        }
        return 0;
    }
    
    @Override
    public int compareTo(final IndexEntry o) {
        return this.compareTo((IndexEntry)o, this.getTerm().length);
    }

	public byte[] getTerm() {
		return term;
	}
}
