package com.shn.dlp.sid.entries;

import java.io.DataInputStream;
import java.io.IOException;

import com.shn.dlp.sid.util.IndexSize;

class CellFactory implements IndexEntryFactory
{    
    @Override
    public int estimateMemory(final int termSize, final int spineLength, final int entryCount) {
        final long inBytes = spineLength * 4L + IndexSize.getSizeOfRowTermContent(termSize, entryCount, spineLength, 0);
        final long inKBytes = (inBytes + 512L) / 1024L;
        if (inKBytes > 2147483647L) {
            throw new IllegalArgumentException("Estimated memory is way too large: " + inKBytes + "KB.");
        }
        return (int)inKBytes;
    }
    
    @Override
    public IndexEntry read(final DataInputStream in, final int termLength) throws IOException {
        final byte[] term = new byte[termLength];
        final int row = in.readInt();
        final byte col = in.readByte();
        in.readFully(term);
        return new Cell(row, col, term);
    }
}
