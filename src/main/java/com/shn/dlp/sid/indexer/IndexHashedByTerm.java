package com.shn.dlp.sid.indexer;

import com.shn.dlp.sid.entries.IndexEntry;

abstract class IndexHashedByTerm extends Index
{
    IndexHashedByTerm(final int targetEntryCount, final double targetColisionListLength, final int termLengthToRetain) {
        super(targetEntryCount, targetColisionListLength, termLengthToRetain);
    }
    
    @Override
    final int calculateBucket(final IndexEntry entry) {
        return Index.getFourBytesHash(entry) % this.getSpineLength();
    }
}
