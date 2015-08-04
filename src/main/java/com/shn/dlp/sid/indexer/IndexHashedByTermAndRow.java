package com.shn.dlp.sid.indexer;

import com.shn.dlp.sid.entries.Cell;
import com.shn.dlp.sid.entries.CommonTermCell;
import com.shn.dlp.sid.entries.IndexEntry;

final class IndexHashedByTermAndRow extends Index
{
    IndexHashedByTermAndRow(final int targetEntryCount, final double targetColisionListLength, final int termLengthToRetain) {
        super(targetEntryCount, targetColisionListLength, termLengthToRetain);
    }
    
    @Override
    void addEntry(final IndexEntry entry) {
        super.addEntry(new CommonTermCell((Cell)entry));
    }
    
    @Override
    int calculateBucket(final IndexEntry cell) {
        final int bucket = (Index.getFourBytesHash(cell) % this.getSpineLength() + ((Cell)cell).getRow()) % this.getSpineLength();
        return bucket;
    }
}