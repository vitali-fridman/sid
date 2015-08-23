package com.shn.dlp.sid.entries;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public final class AllCommonTermsEntry extends IndexEntry {
	
	private final int colMask;

	AllCommonTermsEntry(final int colMask,  final byte[] term) {
		super(term);
		this.colMask = colMask;
	}
	
	public int getColMask() {
		return this.colMask;
	}

	public static AllCommonTermsEntry read(final DataInputStream in, final int termLength) throws IOException {
        final byte[] term = new byte[termLength];
        final int colMask = in.readInt();
        in.readFully(term);
        return new AllCommonTermsEntry(colMask, term);
    }
    
    @Override
    public void write(final DataOutputStream out, final int termLength) throws IOException {
        out.writeInt(this.colMask);
        out.write(this.getTerm(), 0, termLength);
    }

}
