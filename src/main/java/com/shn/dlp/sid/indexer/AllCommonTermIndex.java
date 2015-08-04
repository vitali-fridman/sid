package com.shn.dlp.sid.indexer;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.io.DataOutputStream;

import com.shn.dlp.sid.entries.IndexEntry;

final class AllCommonTermIndex extends IndexHashedByTerm
{
    AllCommonTermIndex(final int targetEntryCount, final int targetColisionListLength) {
        super(targetEntryCount, targetColisionListLength, 0);
    }
    
    private static int mask(final int column) {
        if (column > 30) {
            throw new IllegalArgumentException("Column out of range: " + column);
        }
        return 1 << column;
    }
    
    static int addToMask(final int oldMask, final int column) {
        if (column > 30) {
            throw new IllegalArgumentException("Column out of range: " + column);
        }
        return oldMask | mask(column);
    }
    
    @Override
    void writeToFile(final DataOutputStream indexFile) throws IOException {
        final int termToRetain = 20;
        indexFile.writeInt(termToRetain);
        indexFile.writeInt(this._spine.length);
        indexFile.writeInt(this._emptyBucketCount);
        if (this._entryCount == -1) {
            throw new IllegalStateException("Entry count for index is not set");
        }
        indexFile.writeInt(this._entryCount);
        for (int i = 0; i < this._spine.length; ++i) {
            final List<IndexEntry> bucket = this._spine[i];
            if (bucket != null) {
                indexFile.writeInt(bucket.size());
                final Iterator<IndexEntry> it = bucket.iterator();
                while (it.hasNext()) {
                	IndexEntry ie = it.next();
                	ie.write(indexFile, termToRetain);
                }
            }
            else {
                indexFile.writeInt(0);
            }
        }
    }
}

