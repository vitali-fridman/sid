package com.shn.dlp.sid.entries;

import java.io.DataInputStream;
import java.io.IOException;

import com.shn.dlp.sid.util.IndexSize;


public class AllCommonTermsIndexEntryFactory implements IndexEntryFactory {

	@Override
    public int estimateMemory(final int termSize, final int spineLength, final int entryCount) {
        return (4 * spineLength + IndexSize.getSizeOfAllCommonTermContent(termSize, entryCount, spineLength, 0) + 512) / 1024;
    }
    
    @Override
    public IndexEntry read(final DataInputStream in, final int termLength) throws IOException {
        final byte[] term = new byte[termLength];
        final int colMask = in.readInt();
        in.readFully(term);
        return new AllCommonTermsIndexEntry(colMask, term);
    }

}
